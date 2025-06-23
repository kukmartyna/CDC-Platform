import json
import os
from io import BytesIO

import boto3
import fastavro
import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType
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


def load_kafka_dataframe(spark):
    kafka_options = {
        'kafka.bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,  # 'kafka:9092'
        'subscribePattern': rf'{TOPIC_PREFIX}.{SCHEMA_NAME}.*',
        "startingOffsets": "earliest"
    }
    return spark.readStream.format('kafka').options(**kafka_options).load()


def get_schema_by_id(schema_id):
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    resp = requests.get(url)
    resp.raise_for_status()

    schema_str = resp.json()['schema']
    try:
        schema_dict = fastavro.parse_schema(eval(schema_str))
    except:
        schema_dict = fastavro.parse_schema(json.loads(schema_str))

    return schema_dict


def decode_avro_binary(payload):
    if payload is None or len(payload) < 5:
        return None
    payload = bytes(payload)
    if payload[0] != 0:
        return None

    schema_id = int.from_bytes(payload[1:5], byteorder='big')
    schema = get_schema_by_id(schema_id)

    try:
        record = fastavro.schemaless_reader(BytesIO(payload[5:]), schema)
        return str(record)
    except Exception as e:
        return f"Error: {e}"


@pandas_udf(StringType())
def decode_avro_udf(series: pd.Series) -> pd.Series:
    return series.apply(decode_avro_binary)


def main():
    ensure_minio_bucket()
    spark = get_spark()

    df = load_kafka_dataframe(spark)
    df = df.withColumn("value", decode_avro_udf(col("value")))

    df.writeStream \
        .format("delta") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .option("path", S3A_PATH) \
        .outputMode("append") \
        .partitionBy("topic") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    main()
