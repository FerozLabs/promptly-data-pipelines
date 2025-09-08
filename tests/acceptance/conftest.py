import os
import subprocess
import tempfile
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
STATUS_CODE_CREATED = 201
CURRENT_DIR = os.path.dirname(__file__)


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


@pytest.fixture(scope='session')
def kind_cluster():
    # Ensure brew is installed
    subprocess.run(
        'which brew || /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"',
        shell=True,
        check=True,  # noqa: E501
    )

    # Ensure kind is installed
    subprocess.run('which kind || brew install kind', shell=True, check=True)

    # Verify if a kind cluster named 'acceptance' already exists
    existing_clusters = subprocess.run(
        'kind get clusters',
        check=False,
        shell=True,
        capture_output=True,
        text=True,
    )
    if 'acceptance' in existing_clusters.stdout.splitlines():
        subprocess.run(
            'kind delete cluster --name acceptance',
            shell=True,
            check=True,
        )

    # Create kind cluster
    cluster_creation = subprocess.run(
        'kind create cluster --config tests/acceptance/fixtures/infra/kind_cluster.yaml --name acceptance --wait 3m',  # noqa: E501
        shell=True,
        check=True,
    )

    assert cluster_creation.returncode == 0, (
        'Kind cluster creation failed or timed out'
    )

    # Ensure kubectl is installed
    subprocess.run(
        'which kubectl || brew install kubectl', shell=True, check=True
    )

    # Ensure kubectx is installed
    subprocess.run(
        'which kubectx || brew install kubectx',
        shell=True,
        check=True,
    )

    # Switch to kind context
    subprocess.run('kubectx kind-acceptance', shell=True, check=True)

    # Ensure Helm is installed
    subprocess.run('which helm || brew install helm', shell=True, check=True)

    yield

    # Delete kind cluster
    subprocess.run(
        'kind delete cluster --name acceptance', shell=True, check=True
    )


@pytest.fixture(scope='session')
def gitea_container():
    # Start Gitea container
    container = (
        DockerContainer('gitea/gitea:latest')
        .with_exposed_ports(3000, 22)
        .with_env('USER_UID', '1000')
        .with_env('USER_GID', '1000')
        .with_env('GITEA__database__DB_TYPE', 'sqlite3')
        .with_env('GITEA__database__PATH', '/data/gitea/gitea.db')
        .with_env('GITEA__security__INSTALL_LOCK', 'true')
        .with_env('GITEA__security__SECRET_KEY', 'infra-acceptance-test')
        .with_env('GITEA__security__INTERNAL_TOKEN', 'infra-acceptance-test')
        .with_env('GITEA__service__DISABLE_REGISTRATION', 'true')
        .with_env('GITEA__service__ENABLE_NOTIFY_MAIL', 'false')
        .with_env('GITEA__admin__USERNAME', 'admin')
        .with_env('GITEA__admin__PASSWORD', 'password123')
        .with_env('GITEA__admin__EMAIL', 'admin@example.com')
    )

    container.start()

    # Get connection details
    host = container.get_container_host_ip()
    web_port = container.get_exposed_port(3000)
    ssh_port = container.get_exposed_port(22)

    # Wait for Gitea to start (simple sleep)
    time.sleep(10)

    os.environ['GITEA_WEB_URL'] = f'http://{host}:{web_port}'
    os.environ['GITEA_SSH_URL'] = f'ssh://git@{host}:{ssh_port}/'

    container.exec([
        'su',
        'git',
        '-c',
        'gitea admin user create --username admin --password password123 --email admin@example.com --admin',  # noqa: E501
    ])

    response = requests.post(
        f'{os.environ["GITEA_WEB_URL"]}/api/v1/users/admin/tokens',
        auth=('admin', 'password123'),
        json={
            'name': 'acceptance-test-token',
            'scopes': ['all'],  # scopes válidos na versão atual
        },
    )

    os.environ['GITEA_ADMIN_TOKEN'] = response.json()['sha1']

    yield container

    container.stop()


@pytest.fixture(scope='session')
def gitea_repo(gitea_container: DockerContainer):
    gitea_url = os.environ['GITEA_WEB_URL']
    # create admin token via api

    response = requests.post(
        f'{gitea_url}/api/v1/user/repos',
        json={
            'name': 'infra-acceptance-test',
            'private': True,
            'auto_init': True,
        },
        headers={'Authorization': f'token {os.environ["GITEA_ADMIN_TOKEN"]}'},
    )

    assert response.status_code == STATUS_CODE_CREATED, (
        'Failed to create Gitea repository'
    )

    return response.json()


@pytest.fixture
def gitea_repo_with_all_current_changes(gitea_repo: dict):
    gitea_url = os.environ['GITEA_WEB_URL']
    token = os.environ['GITEA_ADMIN_TOKEN']
    repo_url = f'{gitea_url}/{gitea_repo["full_name"]}.git'
    authed_url = repo_url.replace('://', f'://admin:{token}@')

    tmpdir = tempfile.mkdtemp()

    # copia tudo (inclusive arquivos não rastreados)
    subprocess.run(
        f"rsync -av --exclude '.git' ./ {tmpdir}/",
        shell=True,
        check=True,
    )

    subprocess.run('git init', cwd=tmpdir, shell=True, check=True)
    subprocess.run(
        "git config user.email 'ci@example.com'",
        cwd=tmpdir,
        shell=True,
        check=True,
    )
    subprocess.run(
        "git config user.name 'CI Bot'",
        cwd=tmpdir,
        shell=True,
        check=True,
    )
    subprocess.run('git add .', cwd=tmpdir, shell=True, check=True)
    subprocess.run(
        "git commit -m 'Test commit from local state'",
        cwd=tmpdir,
        shell=True,
        check=True,
    )
    subprocess.run(
        f'git remote add origin {authed_url}',
        cwd=tmpdir,
        shell=True,
        check=True,
    )
    subprocess.run(
        'git push origin master --force',
        cwd=tmpdir,
        shell=True,
        check=True,
    )

    local_commit = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        cwd=tmpdir,
        text=True,
        check=True,
    ).stdout.strip()

    remote_commit = subprocess.run(
        ['git', 'ls-remote', authed_url, 'master'],
        capture_output=True,
        cwd=tmpdir,
        text=True,
        check=True,
    ).stdout.split()[0]

    assert local_commit == remote_commit, (
        f'Expected {local_commit}, got {remote_commit}'
    )

    return authed_url
