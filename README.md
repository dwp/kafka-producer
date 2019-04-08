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
`python3 main.py --aws-profile dataworks-development --kafka-bootstrap-servers localhost:29092 --topic-prefix db.`

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
