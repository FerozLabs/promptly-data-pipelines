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

    setup_argo_cd(
        repo_url=gitea_repo_with_all_current_changes,
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
    assert kind_argo_cd_url_repo.stdout == gitea_repo_with_all_current_changes

    # TODO: Verify if Argo CD installed Trino with proper values

    # TODO: Verify if Argo CD installed Airflow with proper values
