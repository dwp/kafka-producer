#!/usr/bin/env python3

import argparse
import boto3
import json
import logging
import os
import sys

from kafka import KafkaProducer

# Initialise logging
logger = logging.getLogger(__name__)
log_level = os.environ["LOG_LEVEL"] if "LOG_LEVEL" in os.environ else "ERROR"
logger.setLevel(logging.getLevelName(log_level.upper()))
logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(module)s "
    "%(process)s[%(thread)s] %(message)s",
)
logger.info("Logging at {} level".format(log_level.upper()))


def get_parameters():
    parser = argparse.ArgumentParser(
        description="Convert S3 objects into Kafka messages"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--kafka-bootstrap-servers")
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-bucket-prefix")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]

    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]

    if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
        _args.kafka_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    if "S3_BUCKET" in os.environ:
        _args.s3_bucket = os.environ["S3_BUCKET"]

    if "S3_BUCKET_PREFIX" in os.environ:
        _args.s3_bucket_prefix = os.environ["S3_BUCKET_PREFIX"]

    missing_args = False
    if not _args.kafka_bootstrap_servers:
        missing_args = True
        logger.error(
            "Missing required argument: --kafka_bootstrap_servers (KAFKA_BOOTSTRAP_SERVERS)"
        )

    if not _args.s3_bucket:
        missing_args = True
        logger.error("Missing required argument: --s3-bucket (S3_BUCKET)")

    if not _args.s3_bucket_prefix:
        missing_args = True
        logger.error("Missing required argument: --s3-bucket-prefix (S3_BUCKET_PREFIX)")

    if missing_args:
        raise argparse.ArgumentError(None, "Missing required argument(s)")

    return _args


def handler(event, context):
    try:
        args = get_parameters()
    except argparse.ArgumentError as e:
        raise

    try:
        produce_kafka_messages(event, args)
    except KeyError as key_name:
        logger.error(f"Key: {key_name} is required in payload")


def get_s3_keys(bucket, prefix):
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    logger.debug(f"Processing prefix: {prefix}")
    for page in s3_paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get("Contents", ()):
            yield content["Key"]


def produce_kafka_messages(event, args):
    boto3.setup_default_session(
        profile_name=args.aws_profile, region_name=args.aws_region
    )

    if logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        logger.debug(f"Using boto3 {boto3.__version__}")

    logger.debug(event)

    message = json.loads(event["Records"][0]["Sns"]["Message"])
    logger.debug(message)
    if "fixture_data" not in message:
        raise KeyError("fixture_data")

    # {
    #     "job_id": "aws-ingest_upload-fixture-data-dev_13",
    #     "bucket": "abcdefg",
    #     "fixture_data": [
    #         "test-messages/functional_a",
    #         "test-messages/functional_b"
    #     ]
    # }

    # Update dynamo db record
    update_job_status(message["job_id"], "RUNNING")

    # Process each fixture data dir
    producer = KafkaProducer(bootstrap_servers=args.kafka_bootstrap_servers)
    s3_client = boto3.client("s3")
    for prefix in message["fixture_data"]:
        for s3_key in get_s3_keys(args.s3_bucket, prefix):
            logger.debug(f"Processing key: {s3_key}")
            line_no = 0
            for line in (
                s3_client.get_object(Bucket=args.s3_bucket, Key=s3_key)["Body"]
                .read()
                .splitlines()
            ):
                line_no += 1
                try:
                    json.loads(line)
                except json.JSONDecodeError as e:
                    logger.error(
                        f"line {line_no} of {key} contains invalid JSON data: {e.msg}"
                    )
                    continue

                producer.send(line)

    # Update status on dynamo db record
    update_job_status(message["job_id"], "COMPLETE")


def update_job_status(job_id, job_status):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("JobStatus")

    response = table.update_item(
        Key={"JobId": job_id},
        UpdateExpression="set JobStatus = :s",
        ExpressionAttributeValues={":s": job_status},
        ReturnValues="UPDATED_NEW",
    )
    return response


if __name__ == "__main__":
    try:
        json_content = json.loads(open("event.json", "r").read())
        handler(json_content, None)
    except Exception as e:
        logger.error(e)
