import argparse
import random
import boto3
from internet_scholar import read_dict_from_s3_url, AthenaLogger, AthenaDatabase, instantiate_ec2
import logging
import csv
from pathlib import Path
import json

SELECT_DISTINCT_CHANNEL = """
select distinct
  snippet.channelId as channel_id
from
  youtube_video_snippet
where
  snippet.channelId is not NULL and
  snippet.channelId not in (
    select
      youtube_channel_stats.id
    from
      youtube_channel_stats
    where
      youtube_channel_stats.creation_date = cast(current_date as varchar)
    )
"""

SELECT_COUNT_DISTINCT_CHANNEL = """
select count(distinct snippet.channelId) as channel_count
from
  youtube_video_snippet
where
  snippet.channelId is not NULL and
  snippet.channelId not in (
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

    QUEUING_INTERVAL = 1000
    WAIT_WHEN_SERVICE_UNAVAILABLE = 30
    WAIT_WHEN_CONNECTION_RESET_BY_PEER = 60

    def collect_channel_stats(self):
        logging.info("Start collecting Youtube channel stats - producer")
        channel_ids = Path(Path(__file__).parent, 'tmp', 'channel_ids.csv')

        sqs = boto3.resource('sqs')
        credentials_queue = sqs.get_queue_by_name(QueueName='youtube_credentials')
        credentials_queue.purge()
        logging.info('Cleaned queue for credentials')
        for credential in self.credentials:
            credentials_queue.send_message(MessageBody=credential['developer_key'])
        logging.info('Enqueued credentials')

        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)
        logging.info("Recreate table for Youtube channel stats")
        athena.query_athena_and_wait(query_string="DROP TABLE IF EXISTS youtube_channel_stats")
        athena.query_athena_and_wait(query_string=CREATE_CHANNEL_STATS_JSON.format(s3_bucket=self.s3_data))
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE youtube_channel_stats")
        athena.query_athena_and_download(query_string=SELECT_DISTINCT_CHANNEL, filename=channel_ids)
        channel_count = int(
            athena.query_athena_and_get_result(
                query_string=SELECT_COUNT_DISTINCT_CHANNEL
            )['channel_count']
        )
        logging.info("There are %d channels to be processed: download them", channel_count)

        with open(channel_ids, newline='') as csv_reader:
            reader = csv.DictReader(csv_reader)
            channels_queue = sqs.get_queue_by_name(QueueName='youtube_channels')
            channels_queue.purge()
            logging.info('Cleaned queue for channels')
            channels_message = list()
            for channel_id in reader:
                channels_message.append(channel_id)
                if len(channels_message) == self.QUEUING_INTERVAL:
                    channels_queue.send_message(MessageBody=json.dumps(channels_message))
                    channels_message = list()
                    logging.info("Added %d channels to the queue", self.QUEUING_INTERVAL)
            if len(channels_message) != 0:
                channels_queue.send_message(MessageBody=json.dumps(channels_message))
                logging.info("Added %d channels to the queue", len(channels_message))

        logging.info("Concluded collecting channel stats - producer")


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
