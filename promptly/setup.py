import os

import loguru
from sqlalchemy import text

from promptly.adapters.data.postgres.datagen import ingest_fake_data
from promptly.settings import Settings, configure_settings

logger = loguru.logger


def iceberg_with_nessie_catalog_ddl(settings: Settings):
    return """
    CREATE CATALOG iceberg USING iceberg
        WITH (
            "iceberg.catalog.type"='nessie',
            "iceberg.nessie-catalog.uri"='http://nessie:19120/api/v1',
            "iceberg.nessie-catalog.default-warehouse-dir"='s3://iceberg/warehouse',
            "iceberg.nessie-catalog.ref"='main',
            "fs.native-s3.enabled"='true',
            "s3.endpoint"='http://minio:9000',
            "s3.aws-access-key"='minioadmin',
            "s3.aws-secret-key"='minioadmin',
            "s3.region"='us-west-1',
            "s3.path-style-access"='true'
    )
    """


def s3_catalog_ddl(settings: Settings):
    return """
    CREATE CATALOG s3 USING hive
        WITH (
            "hive.metastore" = 'file',
            "fs.native-s3.enabled"='true',
            "fs.native-local.enabled"='true',
            "s3.endpoint"='http://minio:9000',
            "hive.metastore.catalog.dir" = 'file:///tmp/trino-metastore',
            "s3.aws-access-key"='minioadmin',
            "s3.aws-secret-key"='minioadmin',
            "s3.region"='us-west-1',
            "s3.path-style-access"='true'
    )
    """


def external_healthcare_db_ddl(settings: Settings):
    return """
    CREATE CATALOG postgres_healthcare_db USING postgresql
    WITH (
    "connection-url"='jdbc:postgresql://postgres_medical:5432/test',
    "connection-user"='test',
    "connection-password"='test'
    )
    """


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

    # Configure Trino catalogs
    settings.trino_cluster.create_catalog_if_not_exists(
        'iceberg', iceberg_with_nessie_catalog_ddl(settings)
    )

    settings.trino_cluster.create_catalog_if_not_exists(
        's3', s3_catalog_ddl(settings)
    )

    settings.trino_cluster.create_catalog_if_not_exists(
        'postgres_healthcare_db', external_healthcare_db_ddl(settings)
    )

    logger.info('Trino catalogs configured successfully.')

    # Populate Postgres
    populate_postgres_with_medical_data_sample(settings)

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
        external_location = 's3://healthcare/raw/'
    )
    """

    settings.trino_cluster.execute_query(create_providers_table)


if __name__ == '__main__':
    main()
