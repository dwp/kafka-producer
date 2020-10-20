#!/usr/bin/env python3

import argparse
import boto3
import json
import logging
import os
import sys
import requests
import base64
import binascii
import uuid
import socket


from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter
from kafka import KafkaProducer


dynamo_table_name = (
    os.environ["DYNAMO_DB_TABLE_NAME"]
    if "DYNAMO_DB_TABLE_NAME" in os.environ
    else "JobStatus"
)


true_strings = ["True", "true", "TRUE", "1"]
logger_level = os.environ["LOG_LEVEL"] if "LOG_LEVEL" in os.environ else "ERROR"


def setup_logging(logger_level):
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)

    hostname = socket.gethostname()

    json_format = (
        '{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"environment": "{args.environment}", "application": "{args.application}", '
        f'"module": "%(module)s", "process": "%(process)s", '
        f'"thread": "[%(thread)s]", "hostname": "{hostname}" }} '
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level.upper())
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def get_parameters():
    parser = argparse.ArgumentParser(
        description="Convert S3 objects into Kafka messages"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--kafka-bootstrap-servers", default=argparse.SUPPRESS)
    parser.add_argument("--ssl-broker", default="True")
    parser.add_argument("--dks-endpoint", default="NOT_SET")
    parser.add_argument("--encryption-key", default="NOT_SET")
    parser.add_argument("--encrypted-encryption-key", default="NOT_SET")
    parser.add_argument("--master-encryption-key-id", default="NOT_SET")
    parser.add_argument("--environment", default="NOT_SET")
    parser.add_argument("--application", default="NOT_SET")

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

    if "DKS_ENDPOINT" in os.environ:
        _args.dks_endpoint = os.environ["DKS_ENDPOINT"]

    if "ENCRYPTION_KEY" in os.environ:
        _args.encryption_key = os.environ["ENCRYPTION_KEY"]

    if "ENCRYPTED_ENCRYPTION_KEY" in os.environ:
        _args.encrypted_encryption_key = os.environ["ENCRYPTED_ENCRYPTION_KEY"]

    if "MASTER_ENCRYPTION_KEY_ID" in os.environ:
        _args.master_encryption_key_id = os.environ["MASTER_ENCRYPTION_KEY_ID"]

    if "ENVIRONMENT" in os.environ:
        _args.environment = os.environ["ENVIRONMENT"]

    if "APPLICATION" in os.environ:
        _args.application = os.environ["APPLICATION"]

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


args = get_parameters()
logger = setup_logging(logger_level)


def handler(event, context):
    event_string = get_escaped_json_string(event)

    try:
        logger.info(
            f'Event received by lambda", "event_parsed": {event_string}, "dynamo_db_table": "{dynamo_table_name}'
        )
    except:
        logger.info(
            'Event received by lambda but could not be converted to json string", '
            + f'"event": "{event}'
        )

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
        False
        if "kafka_random_key" not in message
        else (message["kafka_random_key"].lower() == "true")
    )
    topic_prefix = (
        ""
        if "topic_prefix" not in message
        else (message["topic_prefix"])
    )

    try:
        produce_kafka_messages(
            message["bucket"],
            message["job_id"],
            message["fixture_data"],
            message["key"],
            message["key"],
            skip_encryption,
            single_topic,
            args,
            produce_x_number_of_messages,
            kafka_random_key,
            topic_prefix,
        )
    except Exception as e:
        logger.error(
            f'Exception occured when producing Kafka messages", "exception": "{e}'
        )
        update_job_status(message["job_id"], "FAILED")
        raise e

    # Update status on dynamo db record
    update_job_status(message["job_id"], "SUCCESS")


def get_s3_keys(bucket, prefix):
    s3_paginator = boto3.client("s3").get_paginator("list_objects_v2")

    logger.debug(f'Processing s3 prefix", "s3_prefix": "{prefix}')
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
    topic_prefix,
):
    # Process each fixture data dir, sending each file in it to kafka as a payload
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap_servers,
        ssl_check_hostname=args.ssl_broker,
    )

    s3_client = boto3.client("s3")
    for s3_key in fixture_data:
        logger.info(
            f'Processing s3 key", "s3_key": "{s3_key}", "dks_endpoint": "{args.dks_endpoint}'
        )

        payload = s3_client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()
        db_name = "missingDb"
        collection_name = "missingCollection"
        dks_endpoint = os.path.join(args.dks_endpoint, "datakey")

        try:
            data = json.loads(payload)
            message = data["message"]
            if "db" in message:
                db_name = message["db"]
            if "collection" in message:
                collection_name = message["collection"]

            data_string = get_escaped_json_string(data)
            logger.info(
                f'Payload parsed", "payload": {data_string}, "dks_endpoint_full": "{dks_endpoint}", '
                + f'"db_name": "{db_name}", "collection_name": "{collection_name}'
            )
        except json.JSONDecodeError as err:
            logger.warning(
                f'Payload contains invalid JSON data so could not parse db or collection", "payload": "{payload}", "error_message": "{err.msg}'
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
                    f'Payload contains invalid JSON data so could be encrypted", "payload": "{payload}", "error_message": "{err.msg}'
                )

        if single_topic:
            topic_name = f"{topic_prefix}{job_id}"
        else:
            topic_name = f"{topic_prefix}{job_id}_{db_name}.{collection_name}"

        logger.info(
            f'Generating and sending messages", "s3_key": "{s3_key}", "topic_name": "{topic_name}", '
            + f'"kafka_bootstrap_servers": "{args.kafka_bootstrap_servers}", '
            + f'"randomise_kafka_key": "{randomise_kafka_key}", "message_volume": "{message_volume}", "dks_endpoint_full": "{dks_endpoint}'
        )

        for message_count in range(1, int(message_volume) + 1):
            if randomise_kafka_key:
                random_uuid = str(uuid.uuid1())
                key_name_modified = random_uuid + "/" + key_name
            else:
                key_name_modified = key_name

            key_bytes = bytes(key_name_modified, "utf-8")

            logger.info(
                f'Generating and sending messages", "s3_key": "{s3_key}", "topic_name": "{topic_name}", '
                + f'"kafka_bootstrap_servers": "{args.kafka_bootstrap_servers}", '
                + f'"randomise_kafka_key": "{randomise_kafka_key}", "message_volume": "{message_volume}", "dks_endpoint_full": "{dks_endpoint}", '
                + f'"key_bytes": "{key_bytes}", "message_count": "{message_count}'
            )

            producer.send(topic=topic_name, value=encrypted_payload, key=key_bytes)

        producer.flush()

        logger.info(
            f'Messages sent", "s3_key": "{s3_key}", "topic_name": "{topic_name}", '
            + f'"kafka_bootstrap_servers": "{args.kafka_bootstrap_servers}", '
            + f'"randomise_kafka_key": "{randomise_kafka_key}", "message_volume": "{message_volume}", "dks_endpoint_full": "{dks_endpoint}'
        )


def encrypt_payload_and_update_message_using_dks(dks_endpoint, message):
    message_string = get_escaped_json_string(message)

    logger.info(
        f'Encrypting message using dks", "message_received": {message_string}, "dks_endpoint": "{dks_endpoint}'
    )

    content = requests.get(dks_endpoint).json()

    encryption_key = content["plaintextDataKey"]
    encrypted_key = content["ciphertextDataKey"]
    master_key_id = content["dataKeyEncryptionKeyId"]

    message["encryption"]["encryptedEncryptionKey"] = encrypted_key
    message["encryption"]["keyEncryptionKeyId"] = master_key_id

    logger.info(
        f'Retrieved key from dks", "encrypted_key": "{encrypted_key}", "master_key_id": "{master_key_id}", "dks_endpoint": "{dks_endpoint}'
    )

    return encrypt_payload(encryption_key, message)


def get_escaped_json_string(json_string):
    try:
        escaped_string = json.dumps(json.dumps(json_string))
    except:
        escaped_string = json.dumps(json_string)

    return escaped_string


def encrypt_payload_and_update_message_using_key(args, message):
    message_string = get_escaped_json_string(message)

    logger.info(
        f'Encrypting message using encryption key", "message_received": {message_string}, "dynamo_db_table": "{dynamo_table_name}'
    )

    if args.encrypted_encryption_key:
        logger.info(
            f'Adding encrypted dataKey to message", "message_received": {message_string}, "encrypted_encryption_key": "{args.encrypted_encryption_key}'
        )
        message["encryption"]["encryptedEncryptionKey"] = args.encrypted_encryption_key

    if args.master_encryption_key_id:
        logger.info(
            f'Adding master key id to message", "message_received": {message_string}, "master_encryption_key_id": "{args.master_encryption_key_id}'
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

        message_string = get_escaped_json_string(message)
        logger.info(
            f'Payload encrypted", "encrypted_message": {message_string}, "dynamo_db_table": "{dynamo_table_name}'
        )
    except json.JSONDecodeError as err:
        logger.warning(
            f'Message contains invalid JSON data in dbObject so could not be encrypted", "error_message": "{err.msg}", "initialisation_vector": "PHONEYVECTOR'
        )
        message["encryption"]["initialisationVector"] = "PHONEYVECTOR"

    return message


def encrypt(key, plaintext):
    plaintext_string = get_escaped_json_string(plaintext)
    logger.info(
        f'Encrypting payload using key", "unencrypted_payload": {plaintext_string}, "encryption_key": "{key}'
    )

    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(key), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))

    return (base64.b64encode(initialisation_vector), base64.b64encode(ciphertext))


def get_message(event):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    message_string = get_escaped_json_string(message)
    logger.debug(
        f'Message parsed from event", "message_received": {message_string}, "dynamo_db_table": "{dynamo_table_name}'
    )

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
        f'Setting dynamo db status of job", "job_status": "{job_status}", "job_id": "{job_id}", "dynamo_table_name": "{dynamo_table_name}'
    )

    response = table.update_item(
        Key={"JobId": job_id},
        UpdateExpression="set JobStatus = :s",
        ExpressionAttributeValues={":s": job_status},
        ReturnValues="UPDATED_NEW",
    )

    logger.debug(
        f'Dynamo db status for job updated", "response": "{response}", "job_id": "{job_id}", "dynamo_table_name": "{dynamo_table_name}'
    )

    return response


if __name__ == "__main__":
    try:
        boto3.setup_default_session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        json_content = json.loads(open("event.json", "r").read())
        handler(json_content, None)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": "{err.msg}')
