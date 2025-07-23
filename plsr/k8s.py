import os
import sys
import subprocess
import shutil
from pathlib import Path

from plsr.console import console

INGRESS_URL = "https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.13.3/deploy/static/provider/cloud/deploy.yaml"
CERTMGR_URL = "https://github.com/cert-manager/cert-manager/releases/download/v1.19.1/cert-manager.yaml"

CONFIG_DIR = Path(__file__).resolve().parent / "configs" / "k8s"
CLUSTER_ISSUER_PATH = CONFIG_DIR / "ClusterIssuer.yaml"
CLUSTER_ISSUER_NAME = "letsencrypt-prod"

def require_kubectl() -> bool:
    if shutil.which("kubectl") is None:
        console.error("kubectl not found; please install kubectl and ensure your kubeconfig is set.")
        return False
    return True

def deploy_exists(namespace: str, deploy_name: str) -> bool:
    result = subprocess.run(
        ["kubectl", "-n", namespace, "get", "deploy", deploy_name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return result.returncode == 0

def clusterissuer_exists() -> bool:
    result = subprocess.run(
        ["kubectl", "get", "clusterissuer", CLUSTER_ISSUER_NAME],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return result.returncode == 0

def install_ingress():
    if not require_kubectl():
        sys.exit(1)
    if deploy_exists("ingress-nginx", "ingress-nginx-controller"):
        console.info("NGINX Ingress is already installed (deploy/ingress-nginx-controller present). Skipping.")
        return
    console.info("Cleaning up old ingress admission jobs (if present)...")
    subprocess.run(["kubectl", "delete", "job", "-n", "ingress-nginx",
                    "ingress-nginx-admission-create", "--ignore-not-found"], check=False)
    subprocess.run(["kubectl", "delete", "job", "-n", "ingress-nginx",
                    "ingress-nginx-admission-patch", "--ignore-not-found"], check=False)
    console.section("Installing NGINX Ingress Controller (pinned)")
    code = console.run(["kubectl", "apply", "-f", INGRESS_URL])
    if code != 0:
        sys.exit(code)
    console.info("Waiting for ingress controller to be ready...")
    subprocess.run(
        ["kubectl", "-n", "ingress-nginx", "rollout", "status", "deploy/ingress-nginx-controller", "--timeout=5m"],
        check=False
    )

def install_cert_manager():
    if not require_kubectl():
        sys.exit(1)
    deployments = ["cert-manager", "cert-manager-webhook", "cert-manager-cainjector"]
    if all(deploy_exists("cert-manager", d) for d in deployments):
        console.info("cert-manager is already installed (deployments present). Skipping.")
        return
    console.section("Installing cert-manager (pinned)")
    code = console.run(["kubectl", "apply", "-f", CERTMGR_URL])
    if code != 0:
        sys.exit(code)
    console.info("Waiting for cert-manager components to be ready...")
    subprocess.run(["kubectl", "-n", "cert-manager", "rollout", "status",
                    "deploy/cert-manager", "--timeout=5m"], check=False)
    subprocess.run(["kubectl", "-n", "cert-manager", "rollout", "status",
                    "deploy/cert-manager-webhook", "--timeout=5m"], check=False)
    subprocess.run(["kubectl", "-n", "cert-manager", "rollout", "status",
                    "deploy/cert-manager-cainjector", "--timeout=5m"], check=False)

def apply_clusterissuer():
    if not require_kubectl():
        sys.exit(1)
    if not CLUSTER_ISSUER_PATH.is_file():
        console.error(f"ClusterIssuer manifest not found at {CLUSTER_ISSUER_PATH}")
        sys.exit(1)
    if clusterissuer_exists():
        console.info(f"ClusterIssuer '{CLUSTER_ISSUER_NAME}' already exists. Skipping.")
        return
    console.section("Applying ClusterIssuer")
    console.run(["kubectl", "apply", "-f", str(CLUSTER_ISSUER_PATH)])

def setup():
    if not require_kubectl():
        sys.exit(1)
    if (deploy_exists("ingress-nginx", "ingress-nginx-controller")
            and deploy_exists("cert-manager", "cert-manager")
            and deploy_exists("cert-manager", "cert-manager-webhook")
            and deploy_exists("cert-manager", "cert-manager-cainjector")
            and clusterissuer_exists()):
        console.info("Kubernetes ingress stack already installed (Ingress, cert-manager, ClusterIssuer). Nothing to do.")
        console.info("Current LB Service:")
        subprocess.run(["kubectl", "-n", "ingress-nginx", "get", "svc", "ingress-nginx-controller", "-o", "wide"], check=False)
        return
    install_ingress()
    install_cert_manager()
    apply_clusterissuer()
    console.success("Done. Check LB IP:")
    subprocess.run(["kubectl", "-n", "ingress-nginx", "get", "svc", "ingress-nginx-controller", "-o", "wide"], check=False)
