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
    parser.add_argument("--kafka-bootstrap-servers", default=argparse.SUPPRESS)
    parser.add_argument("--ssl-broker", default="True")
    parser.add_argument("--topic-prefix", default="")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]

    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]

    if "KAFKA_BOOTSTRAP_SERVERS" in os.environ:
        _args.kafka_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    if "SSL_BROKER" in os.environ:
        _args.ssl_broker = os.environ["SSL_BROKER"]

    if "TOPIC_PREFIX" in os.environ:
        _args.topic_prefix = os.environ["TOPIC_PREFIX"]

    required_args = ["kafka_bootstrap_servers", "ssl_broker"]
    missing_args = []
    for required_message_key in required_args:
        if required_message_key not in _args:
            missing_args.append(required_message_key)
    if missing_args:
        raise argparse.ArgumentError(
            None,
            "ArgumentError: The following required arguments are missing: {}".format(
                ", ".join(missing_args)
            ),
        )

    # Convert any arguments from strings
    true_stings = ["True", "true", "TRUE", "1"]
    _args.ssl_broker = True if _args.ssl_broker in true_stings else False

    return _args


def handler(event, context):
    args = get_parameters()

    if logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        logger.debug(f"Using boto3 {boto3.__version__}")

    logger.info(event)

    message = get_message(event)

    # Update dynamo db record
    update_job_status(message["job_id"], "RUNNING")

    produce_kafka_messages(
        message["bucket"], message["job_id"], message["fixture_data"], args
    )

    # Update status on dynamo db record
    update_job_status(message["job_id"], "SUCCESS")


def get_s3_keys(bucket, prefix):
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    logger.debug(f"Processing prefix: {prefix}")
    for page in s3_paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get("Contents", ()):
            yield content["Key"]


def produce_kafka_messages(bucket, job_id, fixture_data, args):
    # Process each fixture data dir
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap_servers,
        ssl_check_hostname=args.ssl_broker,
    )
    s3_client = boto3.client("s3")
    for prefix in fixture_data:
        for s3_key in get_s3_keys(bucket, prefix):
            logger.debug(f"Processing key: {s3_key}")
            line_no = 0
            for line in (
                s3_client.get_object(Bucket=bucket, Key=s3_key)["Body"]
                .read()
                .splitlines()
            ):
                line_no += 1
                try:
                    data = json.loads(line)
                    db = data["message"]["db"]
                    collection = data["message"]["collection"]
                    topic_name = f"{args.topic_prefix}{job_id}_{db}.{collection}"
                except json.JSONDecodeError as err:
                    logger.error(
                        f"line {line_no} of {s3_key} contains invalid JSON data: {err.msg}"
                    )
                    continue
                producer.send(topic_name, line)
                producer.flush()


def get_message(event):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    logger.debug(message)
    required_message_keys = ["job_id", "bucket", "fixture_data"]
    missing_keys = []
    for required_message_key in required_message_keys:
        if required_message_key not in message:
            missing_keys.append(required_message_key)
    if missing_keys:
        raise KeyError(
            "KeyError: THe following required keys are missing from payload: {}".format(
                ", ".join(missing_keys)
            )
        )
    return message


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
    args = get_parameters()
    try:
        boto3.setup_default_session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        json_content = json.loads(open("event.json", "r").read())
        handler(json_content, None)
    except Exception as e:
        logger.error(e)
