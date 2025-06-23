FROM bitnami/spark

WORKDIR /usr/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026,io.delta:delta-spark_2.12:3.3.1", "main.py"]
