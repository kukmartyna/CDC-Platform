# Kafka consumer - spark job
This project is a streaming application built using Apache Spark Structured Streaming , which 
reads binary Avro-encoded messages from Apache Kafka. 
Decodes them using schemas retrieved from Schema Registry.
Writes the resulting JSON data to MinIO (S3-compatible storage).


## How to start
### Create python environment
```bash
python -m venv venv
```
### Activate venv
```bash
venv\Scripts\activate
```

### Install requirements
```bash
pip install -r requirements.txt
```

### Environment Configuration
Create a .env file in the root directory with the following content:
- MINIO_ENDPOINT
- MINIO_ACCESS_KEY
- MINIO_SECRET_KEY
- MINIO_BUCKET
- KAFKA_BOOTSTRAP_SERVERS
- TOPIC_PREFIX
- SCHEMA_NAME
- SCHEMA_REGISTRY_URL
- CHECKPOINT_LOCATION

### Running the Application
```bash
python main.py
```

## How It Works

### Schema Registry & Avro
Kafka messages use Confluent's wire format (magic byte + 4-byte schema ID). The app retrieves the schema from the Schema Registry using this ID and decodes the message with fastavro.

### Structured Streaming
Spark reads messages in streaming mode and applies a UDF to decode them.

### Writing to MinIO
Spark writes the processed data in DELTA format to MinIO via the s3a:// interface, partitioned by Kafka topic.

