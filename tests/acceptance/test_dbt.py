import os
import subprocess
import time

import loguru
from dotenv import load_dotenv
from testcontainers.core.container import DockerContainer

load_dotenv()

logger = loguru.logger

INFRASTRUCTURE_UPTIME_START = time.time()
TEST_IMAGE_TAG = 'promptly:test'


def build_image(image_tag: str = 'promptly:test'):
    result = subprocess.run(
        ['docker', 'build', '.', '-t', image_tag],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f'Docker build failed:\n{result.stderr}')
    else:
        logger.info(f'✅ Built Docker image {image_tag}')


def run_command_in_container(
    container: DockerContainer, command: str, component_name: str
):
    """
    Runs a command in a container and checks the exit code.
    """
    result = container.exec(command)
    if result.exit_code != 0:
        logs = container.get_logs()
        raise RuntimeError(
            f'{component_name} failed with exit code {result.exit_code}\n'
            f'Message Error: {result.output.decode("utf-8")}\n'
            f'Container Logs: {logs}'
        )
    return result


def test_acceptance_dbt(  # noqa: PLR0914
    docker_network,
    postgres_with_medical_data_sample,
    minio_with_dimensional_raw_data,
    trino_with_nessie_catalog,
):
    """
    Given a PostgreSQL database is running
    When DBT command is executed
    Then the desired tables needs to be created
    """
    build_image(TEST_IMAGE_TAG)
    postgres = postgres_with_medical_data_sample
    minio = minio_with_dimensional_raw_data
    trino = trino_with_nessie_catalog

    logger.info(
        f'Minio can be accessed from host at: http://{minio.get_container_host_ip()}:{minio.get_exposed_port(9000)}'  # noqa:E501
    )

    logger.info(
        f'Postgres can be accessed from host at: postgresql://{os.environ["DB_USER"]}:{os.environ["DB_PASSWORD"]}@{postgres.get_container_host_ip()}:{os.environ["DB_PORT"]}/{os.environ["DB_NAME"]}'  # noqa:E501
    )

    main_app_container = (
        (
            DockerContainer(TEST_IMAGE_TAG)
            .with_network(docker_network)
            .with_envs(
                TRINO_METHOD='none',
                TRINO_USER='test',
                TRINO_PASSWORD='test',
                TRINO_CATALOG='iceberg',
                TRINO_HOST=trino._container.name,
                TRINO_PORT=8080,
                TRINO_SCHEMA='promptly',
                TRINO_DBT_THREADS='1',
            )
        )
        .with_command('tail -f /dev/null')
        .with_name('dbt-acceptance-test-container')
    )

    main_app_container.start()

    # Asserts if all components are accessible from the main app container
    infra_commands = [
        ('curl http://trino:8080/v1/info', 'Trino Acessibility Check'),
        (
            'curl http://catalog:19120/api/v1/trees',
            'Nessie Acessibility Check',
        ),
        ('curl http://minio:9000/', 'Minio Acessibility Check'),
    ]

    for command, component_name in infra_commands:
        logger.info(f'Testing Component: {component_name}')
        run_command_in_container(main_app_container, command, component_name)

    dbt_common_configs = (
        ' --project-dir dbt/promptly/ --profiles-dir dbt/promptly/profiles/'  # noqa: E501
    )
    dbt_commands = [
        (
            'poetry run dbt deps' + dbt_common_configs + ' --target trino',
            'DBT Deps',
        ),
        (
            'poetry run dbt run --select elementary --full-refresh'
            + dbt_common_configs
            + ' --target trino',
            'Elementary Setup',
        ),  # noqa: E501
        # TODO: add kafka cdc tests
        (
            'poetry run dbt run '
            + dbt_common_configs
            + ' --target trino'
            + ' --exclude elementary --exclude tag:raw+',
            'DBT Run',
        ),
        (
            'poetry run dbt test '
            + dbt_common_configs
            + ' --target trino'
            + ' --exclude tag:raw+',
            'DBT Test',
        ),
        (
            'poetry run edr monitor'
            + dbt_common_configs
            + f' --slack-token {os.getenv("ELEMENTARY_SLACK_TOKEN")}'
            + f' --slack-channel-name {os.getenv("ELEMENTARY_SLACK_CHANNEL")}',
            'Elementary Monitor',
        ),
        ('poetry run edr report' + dbt_common_configs, 'Elementary Report'),
    ]

    for command, component_name in dbt_commands:
        logger.info(f'Testing Component: {component_name}')
        run_command_in_container(main_app_container, command, component_name)

    if os.getenv('DEBUG', 'false').lower() == 'true':
        logger.info('Running in DEBUG mode, leaving container running...')
        logger.info(
            f'You can connect to the container using: docker exec -it {main_app_container._container.name} /bin/bash'  # noqa: E501
        )
        logger.info('Press Ctrl+C to stop the test and the container.')
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info('Stopping the container...')

    # copy the reports from the running container to the host machine for inspection # noqa: E501
    subprocess.run(
        [
            'docker',
            'cp',
            f'{main_app_container._container.name}:/app/edr_target/',
            './tests/acceptance/',
        ],
        check=False,
    )

    assert os.path.exists(
        './tests/acceptance/edr_target/elementary_report.html'
    ), (
        'Elementary report directory was not copied'
        + 'from the container to the host machine'
    )

    main_app_container.stop()
