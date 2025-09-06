import os
import time

import boto3
import loguru
import pytest
import requests
from sqlalchemy import create_engine, text
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.trino import TrinoContainer
from trino.dbapi import connect

from tests.acceptance.fixtures.data.postgres.datagen import ingest_fake_data

logger = loguru.logger


STATUS_CODE_OK = 200


@pytest.fixture(scope='session')
def docker_network():
    docker_network = Network()
    docker_network.create()
    return docker_network


@pytest.fixture(scope='session')
def postgres_with_medical_data_sample():
    MAX_ROWS = 2000000
    postgres = PostgresContainer('postgres:latest')
    postgres.start()
    os.environ['DB_HOST'] = postgres.get_container_host_ip()
    os.environ['DB_PORT'] = str(postgres.get_exposed_port(5432))
    os.environ['DB_NAME'] = postgres.dbname
    os.environ['DB_USER'] = postgres.username
    os.environ['DB_PASSWORD'] = postgres.password

    ingest_fake_data(MAX_NUM_ROWS=MAX_ROWS)

    engine = create_engine(
        f'postgresql+psycopg2://{os.environ["DB_USER"]}:{os.environ["DB_PASSWORD"]}@{os.environ["DB_HOST"]}:{os.environ["DB_PORT"]}/{os.environ["DB_NAME"]}'
    )  # noqa:E501

    with engine.connect() as connection:
        result = connection.execute(text('SELECT COUNT(*) FROM provider;'))
        count = result.scalar()
        assert count == MAX_ROWS, (
            f'Expected {MAX_ROWS} rows in provider table, found {count}'
        )

    yield postgres

    postgres.stop()


@pytest.fixture(scope='session')
def minio(docker_network):
    minio = (
        MinioContainer('minio/minio:latest')
        .with_exposed_ports(9000)
        .with_env('MINIO_ROOT_USER', 'minioadmin')
        .with_env('MINIO_ROOT_PASSWORD', 'minioadmin')
        .with_env('MINIO_REGION_NAME', 'us-west-1')
        .with_command('server /data')
        .with_network(docker_network)
        .with_network_aliases('minio')
    )

    minio.start()
    os.environ['MINIO_ENDPOINT'] = minio.get_container_host_ip()
    os.environ['MINIO_PORT'] = str(minio.get_exposed_port(9000))
    os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
    os.environ['MINIO_SECRET_KEY'] = 'minioadmin'

    yield minio
    minio.stop()


@pytest.fixture(scope='session')
def minio_s3_client(minio):
    s3_client = boto3.client(
        's3',
        endpoint_url=f'http://{os.environ["MINIO_ENDPOINT"]}:{os.environ["MINIO_PORT"]}',
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
    )

    return s3_client


@pytest.fixture(scope='session')
def minio_with_dimensional_raw_data(minio, minio_s3_client):
    s3_client = minio_s3_client

    s3_client.create_bucket(Bucket='healthcare')
    current_path = os.path.dirname(__file__)
    csv_path = os.path.join(current_path, 'fixtures/data/minio/sample.csv')

    s3_client.upload_file(
        Key='raw/providers.csv',
        Filename=csv_path,
        Bucket='healthcare',
    )

    assert (
        s3_client.list_objects_v2(Bucket='healthcare')['Contents'][0]['Key']
        == 'raw/providers.csv'
    ), 'CSV upload to Minio failed'

    return minio


@pytest.fixture(scope='session')
def nessie_catalog(docker_network, minio_s3_client, minio):
    s3_client = minio_s3_client

    s3_client.create_bucket(Bucket='iceberg')
    s3_client.create_bucket(Bucket='landing')

    buckets = s3_client.list_buckets()['Buckets']
    assert buckets[-1]['Name'] == 'landing'
    assert buckets[-2]['Name'] == 'iceberg'

    s3_client.put_object(Bucket='iceberg', Key='warehouse/', Body=b'')

    current_path = os.path.dirname(__file__)
    data_path = os.path.join(current_path, 'fixtures/data/minio/sample.csv')

    s3_client.upload_file(
        Key='iceberg/demo/sample.csv',
        Filename=data_path,
        Bucket='landing',
    )

    nessie_postgres = (
        PostgresContainer('postgres:16')
        .with_network(docker_network)
        .with_network_aliases('nessie_postgres')
    )

    nessie_postgres.start()

    # Config inspired from https://github.com/projectnessie/nessie/blob/main/docker/all-in-one/docker-compose.yml

    s3_url = f'http://{minio.get_container_host_ip()}:{minio.get_exposed_port(9000)}'
    nessie = (
        DockerContainer('ghcr.io/projectnessie/nessie:0.104.5')
        .with_network(docker_network)
        .with_network_aliases('catalog')
        .with_envs(
            AWS_DEFAULT_REGION='us-west-1',
            AWS_ACCESS_KEY_ID=minio.access_key,
            AWS_SECRET_ACCESS_KEY=minio.secret_key,
            NESSIE_SERVER_AUTHENTICATION_ENABLED='false',
            NESSIE_VERSION_STORE_TYPE='JDBC',
            NESSIE_VERSION_STORE_PERSIST_JDBC_DATASOURCE='postgresql',
            QUARKUS_DATASOURCE_POSTGRESQL_JDBC_URL='jdbc:postgresql://nessie_postgres:5432/test',
            QUARKUS_DATASOURCE_POSTGRESQL_USERNAME='test',
            QUARKUS_DATASOURCE_POSTGRESQL_PASSWORD='test',
            NESSIE_CATALOG_DEFAULT_BRANCH='main',
            NESSIE_CATALOG_DEFAULT_WAREHOUSE='iceberg',
            NESSIE_CATALOG_WAREHOUSES_ICEBERG_LOCATION='s3://iceberg/',
            NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT=s3_url,
            NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_REGION='us-west-1',
            NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_PATH_STYLE_ACCESS='true',
            NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ACCESS_KEY='urn:nessie-secret:quarkus:nessie.catalog.secrets.access-key',
            NESSIE_CATALOG_SECRETS_ACCESS_KEY_NAME=minio.access_key,
            NESSIE_CATALOG_SECRETS_ACCESS_KEY_SECRET=minio.secret_key,
            NESSIE_CATALOG_SECRETS_REGION_NAME='us-west-1',
        )
        .with_exposed_ports(
            19120,
            9000,
        )
    )

    nessie.start()

    time.sleep(10)
    nessie_health_check_call = requests.get(
        f'http://localhost:{nessie.get_exposed_port(19120)}/api/v1/trees',
        timeout=5,
    )

    logger.info(
        'Nessie health check status code: '
        + f'{nessie_health_check_call.status_code}'
    )

    logger.info(
        f'Access Nessie UI at: http://localhost:{nessie.get_exposed_port(19120)}'
    )

    assert nessie_health_check_call.status_code == STATUS_CODE_OK, (
        'Nessie is not running as expected'
    )

    yield nessie
    nessie.stop()


@pytest.fixture(scope='session')
def trino_with_nessie_catalog(docker_network, minio, nessie_catalog):
    nessie = nessie_catalog  # noqa: F841
    # s3_url = f'http://{minio.get_container_host_ip()}:{minio.get_exposed_port(9000)}'
    s3_url = 'http://minio:9000'

    trino = (
        TrinoContainer('trinodb/trino:475')
        .with_network(docker_network)
        .with_network_aliases('trino')
        .with_envs(
            CATALOG_MANAGEMENT='dynamic',
        )
    )

    trino.start()

    iceberg_catalog_sql_creation = f"""
    CREATE CATALOG iceberg USING iceberg
        WITH (
            "iceberg.catalog.type"='nessie',
            "iceberg.nessie-catalog.uri"='http://catalog:19120/api/v1',
            "iceberg.nessie-catalog.default-warehouse-dir"='s3://iceberg/warehouse',
            "iceberg.nessie-catalog.ref"='main',
            "fs.native-s3.enabled"='true',
            "s3.endpoint"='{s3_url}',
            "s3.aws-access-key"='{minio.access_key}',
            "s3.aws-secret-key"='{minio.secret_key}',
            "s3.region"='us-west-1',
            "s3.path-style-access"='true'
    )
    """

    s3_catalog_sql_creation = f"""
    CREATE CATALOG s3 USING hive
        WITH (
            "hive.metastore" = 'file',
            "fs.native-s3.enabled"='true',
            "s3.endpoint"='{s3_url}',
            "hive.metastore.catalog.dir" = '/tmp/trino-metastore',
            "s3.aws-access-key"='{minio.access_key}',
            "s3.aws-secret-key"='{minio.secret_key}',
            "s3.region"='us-west-1',
            "s3.path-style-access"='true'
    )
    """

    conn = connect(
        host=trino.get_container_host_ip(),
        port=trino.get_exposed_port(trino.port),
        user='test',
    )

    cur = conn.cursor()
    cur.execute(iceberg_catalog_sql_creation)
    cur.execute(s3_catalog_sql_creation)

    catalog_exists = cur.execute('SHOW CATALOGS')

    catalogs = [row[0] for row in catalog_exists.fetchall()]
    assert 'iceberg' in catalogs
    assert 's3' in catalogs

    return trino
