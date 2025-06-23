import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

from debezium import update_or_create_connector


def validate_envs():
    if not DATABASE_USERNAME:
        raise Exception("No database username provided. Please add DATABASE_USERNAME environment variable.")
    if not DATABASE_PASSWORD:
        raise Exception("No database password provided. Please add DATABASE_PASSWORD environment variable.")
    if not DATABASE_HOST:
        raise Exception("No database host provided. Please add DATABASE_HOST environment variable.")
    if not DATABASE_PORT:
        raise Exception("No database port provided. Please add DATABASE_PORT environment variable.")
    if not DATABASE_NAME:
        raise Exception("No database name provided. Please add DATABASE_NAME environment variable.")
    if not SCHEMA_NAME:
        raise Exception("No schema name provided. Please add SCHEMA_NAME environment variable.")
    if not TOPIC_PREFIX:
        raise Exception("No topic prefix provided. Please add TOPIC_PREFIX environment variable.")
    if not SCHEMA_REGISTRY_URL:
        raise Exception("No schema registry provided. Please add SCHEMA_REGISTRY_URL environment variable.")


load_dotenv()

DATABASE_USERNAME = os.getenv('DB_USER')
DATABASE_PASSWORD = os.getenv('DB_PASS')
DATABASE_HOST = os.getenv('DB_HOST')
DATABASE_PORT = os.getenv('DB_PORT')
DATABASE_NAME = os.getenv('DB_NAME')
SCHEMA_NAME = os.getenv('SCHEMA_NAME')
TOPIC_PREFIX = os.getenv('TOPIC_PREFIX', 'MDP')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL')

validate_envs()


def get_connection():
    connection_string = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'.format(
        username=DATABASE_USERNAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_HOST,
        port=int(DATABASE_PORT),
        database=DATABASE_NAME
    )

    try:
        engine = create_engine(connection_string)
        print('Successfully connected!')
        return engine
    except Exception as e:
        print(e)


def load_csv(file_name: str) -> pd.DataFrame:
    data = pd.read_csv(file_name)
    print(f'Successfully loaded data from {file_name}')
    return data


if __name__ == '__main__':
    print(f"Starting PyProject ...")
    data_path = "./DataSets"
    files = [os.path.join(data_path, f) for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]

    connection = get_connection()

    tables_include_list = []

    for file_path in files:
        table_name = file_path.split('/')[-1].split('.')[0]
        data = load_csv(file_path)
        data.to_sql(con=connection, name=table_name, if_exists='replace', index=False,
                    index_label=data.keys()[0])

        tables_include_list.append(table_name)

    debezium_config = {
        "name": os.getenv('CONNECTOR_NAME', 'default-connector'),
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "plugin.name": "pgoutput",
            "database.hostname": DATABASE_HOST,
            "database.port": DATABASE_PORT,
            "database.user": DATABASE_USERNAME,
            "database.password": DATABASE_PASSWORD,
            "database.dbname": DATABASE_NAME,
            "database.server.name": "postgres",
            "schema.include.list": SCHEMA_NAME,
            "table.include.list": ",".join([f"{SCHEMA_NAME}.{t}" for t in tables_include_list]),
            "topic.prefix": TOPIC_PREFIX,
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": SCHEMA_REGISTRY_URL,
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": SCHEMA_REGISTRY_URL
        }
    }

    update_or_create_connector(debezium_config)
