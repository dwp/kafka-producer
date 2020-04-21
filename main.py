#!/usr/bin/env python3

import argparse
import boto3
import json
import logging
import os
import sys
import happybase
import requests
import thriftpy2
import base64
import binascii
import uuid

from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter
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

dynamo_table_name = (
    os.environ["DYNAMO_DB_TABLE_NAME"]
    if "DYNAMO_DB_TABLE_NAME" in os.environ
    else "JobStatus"
)

true_strings = ["True", "true", "TRUE", "1"]


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
    parser.add_argument("--dks-endpoint", default="")
    parser.add_argument("--encryption-key", default="")
    parser.add_argument("--encrypted-encryption-key", default="")
    parser.add_argument("--master-encryption-key-id", default="")

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

    if "DKS_ENDPOINT" in os.environ:
        _args.dks_endpoint = os.environ["DKS_ENDPOINT"]

    if "ENCRYPTION_KEY" in os.environ:
        _args.encryption_key = os.environ["ENCRYPTION_KEY"]

    if "ENCRYPTED_ENCRYPTION_KEY" in os.environ:
        _args.encrypted_encryption_key = os.environ["ENCRYPTED_ENCRYPTION_KEY"]

    if "MASTER_ENCRYPTION_KEY_ID" in os.environ:
        _args.master_encryption_key_id = os.environ["MASTER_ENCRYPTION_KEY_ID"]

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
    _args.ssl_broker = True if _args.ssl_broker in true_strings else False

    return _args


def handler(event, context):
    args = get_parameters()
    logger.info(args)

    if logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        logger.debug(f"Using boto3 {boto3.__version__}")

    logger.info(event)

    message = get_message(event)

    single_topic = False
    if "single_topic" in message:
        single_topic = True

    # Update dynamo db record
    update_job_status(message["job_id"], "RUNNING")

    skip_encryption = (
            "skip_encryption" in message and message["skip_encryption"] in true_strings
    )

    produce_x_number_of_messages = (
        1 if "kafka_message_volume" not in message else message["kafka_message_volume"]
    )
    kafka_random_key = (
        False if "kafka_random_key" not in message else message["kafka_random_key"]
    )

    produce_kafka_messages(
        message["bucket"],
        message["job_id"],
        message["fixture_data"],
        message["key"],
        skip_encryption,
        single_topic,
        args,
        produce_x_number_of_messages,
        kafka_random_key
    )

    # Update status on dynamo db record
    update_job_status(message["job_id"], "SUCCESS")


def get_s3_keys(bucket, prefix):
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    logger.debug(f"Processing prefix: {prefix}")
    for page in s3_paginator.paginate(Bucket=bucket, Prefix=prefix):
        for content in page.get("Contents", ()):
            yield content["Key"]


def produce_kafka_messages(
        bucket,
        job_id,
        fixture_data,
        key_name,
        skip_encryption,
        single_topic,
        args,
        message_volume,
        randomise_kafka_key,
):
    # Process each fixture data dir, sending each file in it to kafka as a payload
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap_servers,
        ssl_check_hostname=args.ssl_broker,
    )

    s3_client = boto3.client("s3")
    for s3_key in fixture_data:
        logger.info(f"Processing key: {s3_key}")
        logger.info(f"Dks endpoint: {args.dks_endpoint}")
        payload = s3_client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()
        db_name = "missingDb"
        collection_name = "missingCollection"
        dks_endpoint = os.path.join(args.dks_endpoint, "datakey")

        print(f"Payload received: {payload}")

        try:
            data = json.loads(payload)
            message = data["message"]
            if "db" in message:
                db_name = message["db"]
            if "collection" in message:
                collection_name = message["collection"]
        except json.JSONDecodeError as err:
            logger.warning(
                f"File {s3_key} contains invalid JSON data so couldn't get db/collection: Err={err.msg}"
            )

        encrypted_payload = payload
        if not skip_encryption:
            try:
                data = json.loads(payload)
                data["message"] = (
                    encrypt_payload_and_update_message_using_key(args, data["message"])
                    if args.encryption_key
                    else encrypt_payload_and_update_message_using_dks(
                        dks_endpoint, data["message"]
                    )
                )
                encrypted_payload = json.dumps(data).encode("utf-8")
            except json.JSONDecodeError as err:
                logger.warning(
                    f"File {s3_key} contains invalid JSON data so couldn't encrypt payload: Err={err.msg}"
                )

        if single_topic:
            topic_name = f"{args.topic_prefix}{job_id}"
        else:
            topic_name = f"{args.topic_prefix}{job_id}_{db_name}.{collection_name}"

        report = (
            f"file {s3_key} to topic {topic_name} "
            f"at {args.kafka_bootstrap_servers} "
            f"with payload {encrypted_payload}"
        )
        logger.info(f"Sending {report}")
        logger.info(f"Randomise Kafka keys is set to {randomise_kafka_key}")
        logger.info(f"Producing {message_volume} messages to Kafka")
        for message_count in range(1, int(message_volume) + 1):
            if randomise_kafka_key:
                random_uuid = uuid.uuid1().node
                key_name = random_uuid + key_name

            key_bytes = bytes(key_name, "utf-8")
            logger.info(f"Sending message for {key_with_random}")
            producer.send(topic=topic_name, value=encrypted_payload, key=key_bytes)

        producer.flush()
        logger.info(f"Sent {report}")


def encrypt_payload_and_update_message_using_dks(dks_endpoint, message):
    logger.info(f"Encrypting message using endpoint '{dks_endpoint}'")

    content = requests.get(dks_endpoint).json()

    encryption_key = content["plaintextDataKey"]
    encrypted_key = content["ciphertextDataKey"]
    master_key_id = content["dataKeyEncryptionKeyId"]
    logger.info(f"Encrypted dataKey '{encrypted_key}'")

    message["encryption"]["encryptedEncryptionKey"] = encrypted_key
    message["encryption"]["keyEncryptionKeyId"] = master_key_id
    return encrypt_payload(encryption_key, message)


def encrypt_payload_and_update_message_using_key(args, message):
    logger.info(f"Encrypting message using encryption key")

    if args.encrypted_encryption_key:
        logger.info(
            f"Adding encrypted dataKey '{args.encrypted_encryption_key}' to message"
        )
        message["encryption"]["encryptedEncryptionKey"] = args.encrypted_encryption_key

    if args.master_encryption_key_id:
        logger.info(
            f"Adding master key id '{args.master_encryption_key_id}' to message"
        )
        message["encryption"]["keyEncryptionKeyId"] = args.master_encryption_key_id

    return encrypt_payload(args.encryption_key, message)


def encrypt_payload(encryption_key, message):
    try:
        db_object = message["dbObject"]
        record_string = json.dumps(db_object)
        [iv, encrypted_record] = encrypt(encryption_key, record_string)
        message["dbObject"] = encrypted_record.decode("ascii")
        message["encryption"]["initialisationVector"] = iv.decode("ascii")
    except json.JSONDecodeError as err:
        logger.warning(
            f"Message contains invalid JSON data in dbObject so could not encrypt: Err={err.msg}"
        )
        message["encryption"]["initialisationVector"] = "PHONEYVECTOR"

    return message


def encrypt(key, plaintext):
    logger.info(f"Encrypting payload of '{plaintext}' using key '{key}'")

    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(key), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))
    return (base64.b64encode(initialisation_vector), base64.b64encode(ciphertext))


def get_message(event):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    logger.debug(message)
    required_message_keys = ["job_id", "bucket", "fixture_data", "key"]
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
    table = dynamodb.Table(dynamo_table_name)

    logger.info(
        f"Setting dynamo status to {job_status} for job id {job_id} in table {dynamo_table_name}"
    )

    response = table.update_item(
        Key={"JobId": job_id},
        UpdateExpression="set JobStatus = :s",
        ExpressionAttributeValues={":s": job_status},
        ReturnValues="UPDATED_NEW",
    )

    logger.info(f"Dynamo response was {response} for job id {job_id}")
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
