# kafka-producer
Converts files in S3 to Kafka messages

## Local Development

### Setup Docker Containers

Use docker compose to stand up local Kafka Broker, Zookeeper and Connect containers
```bash
docker-compose up
```

### Kafka Message Production

Create an `event.json` file from the `event.json.example` file, changing the `Message` body to match your S3 bucket.

Produce messages to your local broker container:
`python3 main.py --aws-profile dataworks-development --kafka-bootstrap-servers localhost:29092 --ssl-broker False --topic-prefix db.`

### Create the Sink Connector

```
cd ../configure-confluent-kafka-consumer
cat > event.json << EOF
{
  "Records": [
    {
      "Sns": {
        "Message": "{\"detail\": { \"containers\": [ { \"networkInterfaces\": [ { \"privateIpv4Address\": \"127.0.0.1\" } ] } ] } }"
      }
    }
  ]
}
EOF
python3 main.py --topics-regex 'db.*' --s3-bucket-name mybucket --topics-dir business-data/kafka
```

### Inspecting the Results
At this point, you should see JSON files in the S3 bucket denoted by the `--s3-bucket-name` parameter passed to the Kafka consumer configuration above.

## AWS Testing

You can test the aws lambda using a test method with a json like follows:

```
{
  "Records": [
    {
      "EventSource": "aws:sns",
      "EventVersion": "1.0",
      "Sns": {
        "Type": "Notification",
        "MessageId": "03e38856-2185-5112-9c3c-ddd4e89c9bd4",
        "TopicArn": "arn:aws:sns:eu-west-2:1234567890:ingest-load-fixture-data",
        "Subject": "None",
        "Message": "{\"job_id\": \"collection.example\", \"topic_prefix\": \"db.\", \"single_topic\": \"true\", \"bucket\": \"123456789123456789\", \"key\": \"aaaa1111-abcd-4567-1234-1234567890a\", \"skip_encryption\": \"false\", \"fixture_data\": [\"fixture-data/business-data/kafka/functional-tests/test_example.json\"], \"kafka_message_volume\": 2000}",
        "Timestamp": "2020-12-09T11:11:21.777Z",
        "MessageAttributes": {}
      }
    }
  ]
}
```
