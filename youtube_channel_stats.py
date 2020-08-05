import argparse
import random
import boto3
from internet_scholar import read_dict_from_s3_url, AthenaLogger, AthenaDatabase, compress
import logging
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import csv
from pathlib import Path
import json
from datetime import datetime
import time
import uuid
from socket import error as SocketError
import errno

SELECT_DISTINCT_CHANNEL = """
select distinct
  snippet.channelId as channel_id
from
  youtube_video_snippet
where
  snippet.channelId is not NULL
"""

SELECT_COUNT_DISTINCT_CHANNEL = """
select count(distinct snippet.channelId) as channel_count
from
  youtube_video_snippet
where
  snippet.channelId is not NULL
"""

EXTRA_CHANNEL = """
  and snippet.channelId not in (
    select
      youtube_channel_stats.id
    from
      youtube_channel_stats
    where
      youtube_channel_stats.creation_date = cast(current_date as varchar)
    )
"""

CREATE_CHANNEL_STATS_JSON = """
create external table if not exists youtube_channel_stats
(
    kind string,
    etag string,
    id   string,
    statistics struct<
        viewCount: bigint,
        commentCount: bigint,
        subscriberCount: bigint,
        hiddenSubscriberCount: boolean,
        videoCount: bigint
    >,
    retrieved_at timestamp
)
PARTITIONED BY (creation_date String)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://{s3_bucket}/youtube_channel_stats/'
TBLPROPERTIES ('has_encrypted_data'='false')
"""


class YoutubeChannelStats:
    def __init__(self, credentials, athena_data, s3_admin, s3_data):
        self.credentials = credentials
        random.shuffle(self.credentials)
        self.athena_data = athena_data
        self.s3_admin = s3_admin
        self.s3_data = s3_data

    LOGGING_INTERVAL = 100
    WAIT_WHEN_SERVICE_UNAVAILABLE = 30
    WAIT_WHEN_CONNECTION_RESET_BY_PEER = 60

    def collect_channel_stats(self):
        logging.info("Start collecting Youtube channel stats")
        channel_ids = Path(Path(__file__).parent, 'tmp', 'channel_ids.csv')
        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)
        if not athena.table_exists('youtube_channel_stats'):
            query = SELECT_DISTINCT_CHANNEL
            query_count = SELECT_COUNT_DISTINCT_CHANNEL
        else:
            query = SELECT_DISTINCT_CHANNEL + EXTRA_CHANNEL
            query_count = SELECT_COUNT_DISTINCT_CHANNEL + EXTRA_CHANNEL
        athena.query_athena_and_download(query_string=query, filename=channel_ids)
        channel_count = int(
            athena.query_athena_and_get_result(
                query_string=query_count
            )['channel_count']
        )
        logging.info("There are %d channels to be processed: download them", channel_count)

        current_key = 0
        youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                  version="v3",
                                                  developerKey=
                                                  self.credentials[current_key]['developer_key'],
                                                  cache_discovery=False)
        with open(channel_ids, newline='') as csv_reader:
            output_json = Path(Path(__file__).parent, 'tmp', 'youtube_channel_stats.json')
            with open(output_json, 'w') as json_writer:
                reader = csv.DictReader(csv_reader)
                num_channels = 0
                for channel_id in reader:
                    if num_channels % self.LOGGING_INTERVAL == 0:
                        logging.info("%d out of %d channels processed", num_channels, channel_count)
                    num_channels = num_channels + 1

                    service_unavailable = 0
                    connection_reset_by_peer = 0
                    no_response = True
                    response = dict()
                    while no_response:
                        try:
                            response = youtube.channels().list(part="statistics",id=channel_id['channel_id']).execute()
                            no_response = False
                        except SocketError as e:
                            if e.errno != errno.ECONNRESET:
                                logging.info("Other socket error!")
                                raise
                            else:
                                connection_reset_by_peer = connection_reset_by_peer + 1
                                logging.info("Connection reset by peer! {}".format(connection_reset_by_peer))
                                if connection_reset_by_peer <= 10:
                                    time.sleep(self.WAIT_WHEN_CONNECTION_RESET_BY_PEER)
                                    youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                              version="v3",
                                                                              developerKey=
                                                                              self.credentials[current_key][
                                                                                  'developer_key'],
                                                                              cache_discovery=False)
                                else:
                                    raise
                        except HttpError as e:
                            if "403" in str(e):
                                logging.info("Invalid {} developer key: {}".format(
                                    current_key,
                                    self.credentials[current_key]['developer_key']))
                                current_key = current_key + 1
                                if current_key >= len(self.credentials):
                                    raise
                                else:
                                    youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                                              version="v3",
                                                                              developerKey=
                                                                              self.credentials[current_key][
                                                                                  'developer_key'],
                                                                              cache_discovery=False)
                            elif ("503" in str(e)) or ("500" in str(e)):
                                if "503" in str(e):
                                    logging.info("Service unavailable")
                                else:  # 500
                                    logging.info("Internal error encountered")
                                service_unavailable = service_unavailable + 1
                                if service_unavailable <= 10:
                                    time.sleep(self.WAIT_WHEN_SERVICE_UNAVAILABLE)
                                else:
                                    raise
                            else:
                                raise
                    for item in response.get('items', []):
                        item['retrieved_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        json_writer.write("{}\n".format(json.dumps(item)))

        logging.info("Compress file %s", output_json)
        compressed_file = compress(filename=output_json, delete_original=True)

        s3 = boto3.resource('s3')
        s3_filename = "youtube_channel_stats/creation_date={}/{}-{}.json.bz2".format(
            datetime.utcnow().strftime("%Y-%m-%d"),
            uuid.uuid4().hex,
            num_channels)
        logging.info("Upload file %s to bucket %s at %s", compressed_file, self.s3_data, s3_filename)
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_filename)

        logging.info("Recreate table for Youtube channel stats")
        athena.query_athena_and_wait(query_string="DROP TABLE IF EXISTS youtube_channel_stats")
        athena.query_athena_and_wait(query_string=CREATE_CHANNEL_STATS_JSON.format(s3_bucket=self.s3_data))
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE youtube_channel_stats")

        logging.info("Concluded collecting channel stats")


def test_api_keys(s3_path):
    config = read_dict_from_s3_url(url=s3_path)
    credentials = config['youtube']
    current_key = 0
    for current_key in range(0, len(credentials)):
        youtube = googleapiclient.discovery.build(serviceName="youtube",
                                                  version="v3",
                                                  developerKey=
                                                  credentials[current_key]['developer_key'],
                                                  cache_discovery=False)
        try:
            print('Email: {}'.format(credentials[current_key]['email']))
            print('Project: {}'.format(credentials[current_key]['project']))
            print('Key: {}'.format(credentials[current_key]['developer_key']))
            youtube.channels().list(part="statistics", id='UCYiM773ssvNMaBHvaWWeIoQ').execute()
            print('OK!')
        except Exception as e:
            print('Error! {}'.format(str(e)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="youtube-channel-stats",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        youtube_channel_stats = YoutubeChannelStats(credentials=config['youtube'],
                                                    athena_data=config['aws']['athena-data'],
                                                    s3_admin=config['aws']['s3-admin'],
                                                    s3_data=config['aws']['s3-data'])
        youtube_channel_stats.collect_channel_stats()
    finally:
        logger.save_to_s3()
        #logger.recreate_athena_table()


if __name__ == '__main__':
    main()
