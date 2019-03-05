import sys
import argparse
import boto3
import logging
import os
import json
import requests

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
    if 'AWS_PROFILE' in os.environ:
        boto3.setup_default_session(profile_name=args.aws_profile,
                                    region_name=args.aws_region)

    if logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        logger.debug(f"Using boto3 {boto3.__version__}")

    logger.debug(event)


if __name__ == "__main__":
    try:
        json_content = json.loads(open('event.json', 'r').read())
        handler(json_content, None)
    except Exception as e:
        logger.error("Unexpected error occurred")
        logger.error(e)
        raise e
