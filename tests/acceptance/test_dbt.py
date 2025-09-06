import os
import subprocess
import time

import ipdb
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


def test_acceptance_dbt_postgres(  # noqa: PLR0914
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
                TRINO_SCHEMA='public',
                TRINO_DBT_THREADS='1',
            )
        )
        .with_command('tail -f /dev/null')
        .with_name('dbt-acceptance-test-container')
    )

    main_app_container.start()

    # Asserts if all components are accessible from the main app container
    trino_response = main_app_container.exec('curl http://trino:8080/v1/info')
    nessie_response = main_app_container.exec(
        'curl http://catalog:19120/api/v1/trees'
    )
    minio_response = main_app_container.exec('curl http://minio:9000/')

    assert trino_response.exit_code == 0, (
        'Trino is not accessible from the main app container. '
        + f'Message Error: {trino_response.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    assert nessie_response.exit_code == 0, (
        'Nessie is not accessible from the main app container. '
        + f'Message Error: {nessie_response.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    assert minio_response.exit_code == 0, (
        'Minio is not accessible from the main app container. '
        + f'Message Error: {minio_response.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    dbt_deps_command = main_app_container.exec(
        'poetry run dbt deps'
        + ' --project-dir dbt/promptly/'
        + ' --profiles-dir dbt/promptly/profiles/'
        + ' --target trino'
    )

    assert dbt_deps_command.exit_code == 0, (
        f'DBT deps failed with exit code {dbt_deps_command.exit_code}\n'
        + f'Message Error: {dbt_deps_command.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    elementary_setup_command = (
        'poetry run dbt run --select elementary'
        + ' --project-dir dbt/promptly/'
        + ' --profiles-dir dbt/promptly/profiles/'
        + ' --target trino'
        + ' --full-refresh'
    )

    elementary_setup_result = main_app_container.exec(elementary_setup_command)

    assert elementary_setup_result.exit_code == 0, (
        'Elementary setup failed with exit code'
        + f'{elementary_setup_result.exit_code}\n'
        + f'Message Error: {elementary_setup_result.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    dbt_run_command = (
        'poetry run dbt run --project-dir dbt/promptly/'
        + ' --profiles-dir dbt/promptly/profiles/'
        + ' --target trino'
    )

    dbt_run_result = main_app_container.exec(dbt_run_command)

    assert dbt_run_result.exit_code == 0, (
        f'DBT run failed with exit code {dbt_run_result.exit_code}\n'
        + f'Message Error: {dbt_run_result.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    dbt_tests = (
        'poetry run dbt test --project-dir dbt/promptly/'
        + ' --profiles-dir dbt/promptly/profiles/'
        + ' --target trino'
    )

    dbt_test_result = main_app_container.exec(dbt_tests)

    assert dbt_test_result.exit_code == 0, (
        f'DBT tests failed with exit code {dbt_test_result.exit_code}\n'
        + f'Message Error: {dbt_test_result.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    elementary_alerts_command = (
        'poetry run edr monitor'
        + f' --project-dir dbt/promptly/ --profiles-dir dbt/promptly/profiles/'
        + f' --slack-token {os.getenv("ELEMENTARY_SLACK_TOKEN")}'
        + f' --slack-channel-name {os.getenv("ELEMENTARY_SLACK_CHANNEL")}'
    )

    elementary_alerts_result = main_app_container.exec(
        elementary_alerts_command
    )

    assert elementary_alerts_result.exit_code == 0, (
        'Elementary check failed with exit code'
        + f'{elementary_alerts_result.exit_code}\n'
        + f'Message Error: {elementary_alerts_result.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

    if os.getenv('DEBUG', 'false').lower() == 'true':
        ipdb.set_trace()  # noqa: E702

    elementary_report_command = 'poetry run edr report --project-dir dbt/promptly/ --profiles-dir dbt/promptly/profiles/'  # noqa: E501

    elementary_report_result = main_app_container.exec(
        elementary_report_command
    )

    assert elementary_report_result.exit_code == 0, (
        'Elementary report failed with exit code'
        + f'{elementary_report_result.exit_code}\n'
        + f'Message Error: {elementary_report_result.output.decode("utf-8")}\n'
        + f'Container Logs: {main_app_container.get_logs()}'
    )

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
