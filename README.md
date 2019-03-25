# kafka-producer
Converts files in S3 to Kafka messages

## Local Development
Use docker compose to stand up local Kafka Broker and Zookeeper containers
```bash
docker compose up
``` 
Create an `event.json` file from the `event.json.example` file, changing the `Message` body to match your S3 bucket.

Either set environment variables or pass arguments to configure `main.py`. Note: you will need to set the Kafka bootstrap servers to `broker` and SSL broker to `False`
