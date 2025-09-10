from testcontainers.compose import DockerCompose


def test_dev_environment():
    with DockerCompose(
        context='.',
        compose_file_name='docker-compose.yml',
        pull=True,
        build=True,
        keep_volumes=True,
        env_file='.env',
        wait=True,
    ) as compose:
        # Here you can add assertions or checks to verify the dev environment is set up correctly  # noqa: E501
        assert compose is not None
        # For example, you might want to check if certain services are running
        # services = compose.get_services()
