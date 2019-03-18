import sys
import argparse
import boto3
import logging
import os
import json

# Initialise logging
logger = logging.getLogger(__name__)
log_level = os.environ['LOG_LEVEL'] if 'LOG_LEVEL' in os.environ else 'ERROR'
logger.setLevel(logging.getLevelName(log_level.upper()))
logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s %(levelname)s %(module)s '
                           '%(process)s[%(thread)s] %(message)s')
logger.info("Logging at {} level".format(log_level.upper()))


def get_parameters():
    parser = argparse.ArgumentParser(
        description="Convert S3 objects into Kafka messages")

    # Parse command line inputs and set defaults
    parser.add_argument('--aws-profile', default='default')
    parser.add_argument('--aws-region', default='eu-west-2')

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if 'AWS_PROFILE' in os.environ:
        _args.aws_profile = os.environ['AWS_PROFILE']

    if 'AWS_REGION' in os.environ:
        _args.aws_region = os.environ['AWS_REGION']

    return _args


def handler(event, context):
    args = get_parameters()
    try:
        produce_kafka_messages(event, args)
    except KeyError as key_name:
        logger.error(f'Key: {key_name} is required in payload')


def produce_kafka_messages(event, args):
    boto3.setup_default_session(profile_name=args.aws_profile,
                                region_name=args.aws_region)

    if logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        logger.debug(f"Using boto3 {boto3.__version__}")

    logger.debug(event)

    message = json.loads(event['Records'][0]['Sns']['Message'])
    logger.debug(message)

    # {
    #     "job_id": "aws-ingest_upload-fixture-data-dev_13",
    #     "bucket": "abcdefg",
    #     "fixture_data": [
    #         "test-messages/functional_a",
    #         "test-messages/functional_b"
    #     ]
    # }

    # Update dynamo db record
    update_job_status(message['job_id'], "RUNNING")

    # Process each fixture data dir

    # Update status on dynamo db record
    update_job_status(message['job_id'], "COMPLETE")


def update_job_status(job_id, job_status):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('JobStatus')

    response = table.update_item(
        Key={
            'JobId': job_id,
        },
        UpdateExpression="set JobStatus = :s",
        ExpressionAttributeValues={
            ':s': job_status
        },
        ReturnValues="UPDATED_NEW"
    )
    return response


if __name__ == "__main__":
    try:
        json_content = json.loads(open('event.json', 'r').read())
        handler(json_content, None)
    except Exception as e:
        logger.error("Unexpected error occurred")
        logger.error(e)
        raise e
