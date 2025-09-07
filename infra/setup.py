import subprocess


def setup_argo_cd():
    # create argo namespace and install argo cd
    subprocess.run(['kubectl', 'create', 'namespace', 'argocd'], check=True)

    subprocess.run(
        [
            'kubectl',
            'apply',
            '-n',
            'argocd',
            '-f',
            'https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml',
        ],
        check=True,
    )

    # Wait for the argo server deployment to be ready
    subprocess.run(
        [
            'kubectl',
            'rollout',
            'status',
            '-n',
            'argocd',
            'deployment/argocd-server',
        ],
        check=True,
    )

    # Check if the argo server is running
    result = subprocess.run(
        [
            'kubectl',
            'get',
            'pods',
            '-n',
            'argocd',
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
        raise RuntimeError('Argo server is not running')
