import os
import subprocess

import loguru
from dotenv import load_dotenv

logger = loguru.logger


def main():
    load_dotenv()

    env_vars = [
        'TRINO_HOST',
        'TRINO_PORT',
        'TRINO_USER',
        'TRINO_PASSWORD',
        'TRINO_CATALOG',
        'TRINO_SCHEMA',
        'TRINO_DBT_THREADS',
    ]

    pre_command = ''.join([f'{var}={os.getenv(var)} ' for var in env_vars])

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
        # TODO: move these to airflow
        (
            'poetry run dbt run --exclude elementary '
            + dbt_common_configs
            + ' --target trino',  # noqa: E501
            'DBT Run',
        ),
        (
            'poetry run dbt test ' + dbt_common_configs + ' --target trino',
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

    for command, description in dbt_commands:
        logger.info(f'Running: {description}')
        logger.debug(f'Full command: {pre_command + command}')
        subprocess.run(pre_command + command, shell=True, check=True)


if __name__ == '__main__':
    main()
