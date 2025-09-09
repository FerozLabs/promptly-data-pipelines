import os
import subprocess
import tempfile

import yaml


def setup_argo_cd(
    repo_url: str = os.environ['ARGO_CD_REPO_URL'],
    git_user: str = os.environ.get('ARGO_CD_GIT_USER', 'admin'),
    git_token: str = os.environ.get('ARGO_CD_GIT_TOKEN', 'password'),
    namespace: str = 'argocd',
    release_name: str = 'argo-cd',
):
    # Create namespace if it doesn't exist
    subprocess.run(
        ['kubectl', 'create', 'namespace', namespace],
        check=False,  # ignore error if it already exists
    )

    # Add Argo Helm repo and update
    subprocess.run(
        [
            'helm',
            'repo',
            'add',
            'argo',
            'https://argoproj.github.io/argo-helm',
        ],
        check=True,
    )
    subprocess.run(['helm', 'repo', 'update'], check=True)

    # Create temporary values.yaml file with repository secret
    values = {
        'configs': {
            'repositories': {
                'git-repo': {
                    'url': repo_url,
                    'username': git_user,
                    'password': git_token,
                }
            },
            # Enable namespace creation by Argo CD
            'cm': {
                'application.namespaces': '*',
            },
        },
        'server': {'service': {'type': 'LoadBalancer'}},
    }

    with tempfile.NamedTemporaryFile(
        mode='w',
        delete=False,
        encoding='utf-8',
    ) as tmpfile:
        yaml.dump(values, tmpfile)
        values_file = tmpfile.name

    # Install Argo CD via Helm using the values.yaml file
    subprocess.run(
        [
            'helm',
            'upgrade',
            '--install',
            release_name,
            'argo/argo-cd',
            '--namespace',
            namespace,
            '-f',
            values_file,
        ],
        check=True,
    )

    # Wait for Argo CD server deployment to be ready
    subprocess.run(
        [
            'kubectl',
            'rollout',
            'status',
            '-n',
            namespace,
            'deployment/argo-cd-argocd-server',
        ],
        check=True,
    )

    # Check if the Argo CD server pod is running
    result = subprocess.run(
        [
            'kubectl',
            'get',
            'pods',
            '-n',
            namespace,
            '-l',
            'app.kubernetes.io/name=argocd-server',
            '-o',
            'jsonpath={.items[0].status.phase}',
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    if result.stdout.strip() != 'Running':
        raise RuntimeError('Argo CD server is not running')
