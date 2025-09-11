import json
import os
import time

import loguru
import requests
from sqlalchemy import text

from promptly.adapters.data.postgres.datagen import ingest_fake_data
from promptly.settings import Settings, configure_settings

logger = loguru.logger


def populate_postgres_with_medical_data_sample(settings: Settings):
    MAX_ROWS = 2_000_000
    ingest_fake_data(MAX_NUM_ROWS=MAX_ROWS, db=settings.health_care_db)

    with settings.health_care_db.engine.connect() as connection:
        result = connection.execute(text('SELECT COUNT(*) FROM provider;'))
        count = result.scalar()

    logger.info(
        f'Postgres is populated with {count} rows in the provider table.'
    )


def upload_sample_csv_to_minio(settings: Settings):
    bucket_name = 'healthcare'
    settings.s3.create_bucket_if_not_exists(bucket_name)

    current_path = os.path.dirname(__file__)
    csv_path = os.path.join(current_path, 'adapters/data/minio/sample.csv')

    settings.s3.upload_file(
        bucket_name=bucket_name,
        object_name='raw/providers.csv',
        file_path=csv_path,
    )
    logger.info('Sample CSV uploaded to MinIO successfully.')


def main():
    settings = configure_settings()
    logger.info('Settings configured successfully.')

    # Upload data to Minio
    upload_sample_csv_to_minio(settings)

    # Setup iceberg bucket
    settings.s3.create_bucket_if_not_exists('iceberg')

    # Populate Postgres
    populate_postgres_with_medical_data_sample(settings)

    # Enable CDC for relevant tables
    settings.health_care_db.configure_user_cdc()

    try:
        settings.health_care_db.is_cdc_enabled()
    except Exception as e:
        logger.error(f'Error checking CDC status: {e}')
        raise

    settings.health_care_db.create_publication_for_table('provider')

    # Create Debezium connector to extract changes from Postgres
    url = 'http://localhost:8083/connectors'
    payload = {
        'name': 'postgres-cdc',
        'config': {
            'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',  # noqa: E501
            'database.hostname': 'postgres_medical',
            'database.port': '5432',
            'database.user': 'test',
            'database.password': 'test',
            'database.dbname': 'test',
            'database.server.name': 'medical_server',
            'plugin.name': 'pgoutput',
            'publication.name': 'healthcare_pub',
            'slot.name': 'debezium_slot',
            'table.include.list': 'public.provider,public.care_site',
            'topic.prefix': 'cdc',
        },
    }

    time.sleep(20)  # Wait for Kafka Connect to be ready
    # TODO: implement a retry mechanism with exponential backoff and timeout
    try:
        response = requests.post(
            url,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(payload),
        )
        response.raise_for_status()
        logger.info('Debezium connector created successfully.')
    except requests.exceptions.RequestException as e:
        logger.error(f'Error creating Debezium connector: {e}')
        raise

    # Create external csv tables in MiniO
    define_default_external_catalog = """
    CREATE SCHEMA s3.default
        WITH (
            location = 's3://healthcare/raw/'
        )
    """

    settings.trino_cluster.execute_query(define_default_external_catalog)

    create_providers_table = """
    CREATE TABLE s3.default.providers (
        ProviderName VARCHAR,
        ProviderID VARCHAR,
        NPI VARCHAR,
        Specialty VARCHAR,
        SiteName VARCHAR,
        SourceID VARCHAR,
        SpecSource VARCHAR,
        IDSource VARCHAR
    )
    WITH (
        format = 'CSV',
        csv_separator = ',',
        external_location = 's3://healthcare/raw/',
        skip_header_line_count = 1
    )
    """

    settings.trino_cluster.execute_query(create_providers_table)


if __name__ == '__main__':
    main()
