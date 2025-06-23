# PyProject

## CSV to Postgres + Debezium Connector

This project automates the process of 
loading CSV files into a PostgreSQL database,
creating (or updating) a Debezium connector to capture change data,
configuring change event publishing to Apache Kafka in Avro format using Schema Registry.

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
- DB_NAME
- DB_USER
- DB_PASS
- DB_HOST
- DB_PORT
- CONNECTOR_NAME
- SCHEMA_NAME
- TOPIC_PREFIX
- SCHEMA_REGISTRY_URL

### Running the Application
```bash
python main.py
```

## How It Works

## CSV Loading 
Each .csv file is treated as a separate table. The table name is based on the file name (without the extension).

## SQLAlchemy
It is used to establish a connection with PostgreSQL.

## Debezium Connector
It is dynamically configured to include all the tables that were loaded.
The connector uses Avro converters and publishes schemas to Schema Registry.
The connector is managed via the update_or_create_connector function, which interacts with Kafka Connect's REST API.