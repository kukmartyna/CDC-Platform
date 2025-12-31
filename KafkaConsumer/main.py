import json
import os
import boto3
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
from dotenv import load_dotenv


def validate_envs():
    if not MINIO_ENDPOINT:
        raise Exception("No MinIO endpoint provided. Please add MINIO_ENDPOINT environment variable.")
    if not MINIO_ACCESS_KEY:
        raise Exception("No MinIO access key provided. Please add MINIO_ACCESS_KEY environment variable.")
    if not MINIO_SECRET_KEY:
        raise Exception("No MinIO secret key provided. Please add MINIO_SECRET_KEY environment variable.")
    if not MINIO_BUCKET:
        raise Exception("No MinIO bucket provided. Please add MINIO_BUCKET environment variable.")
    if not KAFKA_BOOTSTRAP_SERVERS:
        raise Exception("No kafka bootstrap servers provided. Please add KAFKA_BOOTSTRAP_SERVERS environment variable.")
    if not TOPIC_PREFIX:
        raise Exception("No topic prefix provided. Please add TOPIC_PREFIX environment variable.")
    if not SCHEMA_NAME:
        raise Exception("No schema name provided. Please add SCHEMA_NAME environment variable.")
    if not SCHEMA_REGISTRY_URL:
        raise Exception("No schema registry provided. Please add SCHEMA_REGISTRY_URL environment variable.")
    if not CHECKPOINT_LOCATION:
        raise Exception("No checkpoint location provided. Please add CHECKPOINT_LOCATION environment variable.")


load_dotenv()

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'default')
S3A_PATH = f"s3a://{MINIO_BUCKET}/"
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
TOPIC_PREFIX = os.getenv('TOPIC_PREFIX')
SCHEMA_NAME = os.getenv('SCHEMA_NAME')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')
CHECKPOINT_LOCATION = os.getenv('CHECKPOINT_LOCATION', '/tmp/checkpoint')

validate_envs()


def ensure_minio_bucket():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    buckets = s3.list_buckets()
    if not any(b["Name"] == MINIO_BUCKET for b in buckets.get("Buckets", [])):
        print(f"Tworzę bucket: {MINIO_BUCKET}")
        s3.create_bucket(Bucket=MINIO_BUCKET)
    else:
        print(f"Bucket '{MINIO_BUCKET}' już istnieje.")


def get_spark():
    spark = SparkSession.builder \
        .appName("KafkaToMinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark


def load_kafka_dataframe(spark, topic):
    kafka_options = {
        'kafka.bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,  # 'kafka:9092'
        'subscribe': topic,
        "startingOffsets": "earliest"
    }
    return spark.readStream.format('kafka').options(**kafka_options).load()


def get_schema_by_id(schema_id):
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    resp = requests.get(url)
    resp.raise_for_status()

    schema_str = resp.json()['schema']
    return schema_str


def load_config(path: str):
    with open(path, 'r') as file:
        return json.loads(file.read())


def main():
    dry_run = False
    ensure_minio_bucket()
    spark = get_spark()
    configs = load_config('./config.json')

    for config in configs:
        topic_name = config['topic_name']
        schema = get_schema_by_id(config['schema_id'])
        df = load_kafka_dataframe(spark, topic_name)

        df = df.select(
            expr("substring(value, 6)").alias("payload"),
        )

        df = df.select(from_avro(col("payload"), schema).alias("data")).select(col('data.*'))

        if dry_run:
            (df.writeStream
             .format("console")
             .option("truncate", "false")
             .start())
        else:
            (df.writeStream
                .format("delta")
                .option("checkpointLocation", f'{CHECKPOINT_LOCATION}/{topic_name}')
                .option("path", f'{S3A_PATH}/{topic_name}')
                .outputMode("append")
                .start())

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
