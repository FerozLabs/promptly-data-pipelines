import os
import subprocess
import time

from infra.setup import setup_argo_cd


def test_acceptance_infra(gitea_repo_with_all_current_changes, kind_cluster):
    """
    Given a kubernetes cluster and the Github repo is configured
    When the infra setup is done
    Then it should install all the infrastructure components correctly
        - Argo CD
        - Trino
        - Airflow
    """

    # Delay to ensure kind cluster is fully ready
    time.sleep(10)

    gitea_repo_url, gitea_repo_tmpdir = gitea_repo_with_all_current_changes

    setup_argo_cd(
        repo_url=gitea_repo_url,
        git_user='admin',
        git_token=os.environ['GITEA_ADMIN_TOKEN'],
        namespace='argocd',
        release_name='argo-cd',
    )

    # Verify Argo CD server is running and is aiming to the correct repo
    kind_argo_cd_url_repo = subprocess.run(
        [
            'sh',
            '-c',
            "kubectl -n argocd get secret argocd-repo-git-repo -o jsonpath='{.data.url}' | base64 --decode",  # noqa: E501
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert kind_argo_cd_url_repo.stdout == gitea_repo_url

    # # Apply Argo CD Application for Airflow
    # subprocess.run(
    #     [
    #         'kubectl',
    #         'apply',
    #         '-n',
    #         'argocd',
    #         '-f',
    #         os.path.join(gitea_repo_tmpdir, 'infra', 'airflow', 'application.yaml'), # noqa: E501
    #     ],
    #     check=True,
    # )

    # # TODO: Verify if Argo CD installed Airflow with proper values
    # application_yaml_path = os.path.join(
    #     gitea_repo_tmpdir, 'infra', 'airflow', 'application.yaml'
    # )

    # # Replace placeholders in application.yaml with actual values
    # with open(application_yaml_path, 'r') as file:
    #     content = file.read()
    # content = content.replace('${ARGO_CD_REPO_URL}', gitea_repo_url)
    # content = content.replace('${ARGO_CD_REPO_BRANCH}', 'master')

    # with open(application_yaml_path, 'w') as file:
    #     file.write(content)

    # os.listdir(os.path.join(gitea_repo_tmpdir, 'infra', 'airflow'))

    # # TODO: Verify if Argo CD installed Trino with proper values
