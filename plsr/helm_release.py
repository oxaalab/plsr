from __future__ import annotations

import os
import re
import shlex
import shutil
import json
import base64
import tempfile
import subprocess
import time
from pathlib import Path

from .console import console
from .aws import parse_ecr_image_ref, aws_sts_identity, ecr_get_login_password

def _detect_app_root() -> Path:
    env_root = os.getenv("APP_ROOT")
    if env_root:
        p = Path(env_root).expanduser().resolve()
        if p.is_dir():
            return p
    cur = Path.cwd().resolve()
    for p in (cur, *cur.parents):
        if (p / "config.yaml").is_file():
            return p
    return cur

def _read_cfg_text(root: Path) -> str | None:
    cfg = root / "config.yaml"
    return cfg.read_text(encoding="utf-8") if cfg.is_file() else None

def _extract_block(text: str, section: str) -> str:
    m = re.search(rf"(?ms)^\s*{re.escape(section)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", text)
    return m.group("blk") if m else ""

def _kv_from_block(block: str, key: str) -> str | None:
    m = re.search(rf"(?mi)^\s*{re.escape(key)}\s*:\s*['\"]?([^#\r\n'\"]+)['\"]?\s*(?:#.*)?$", block)
    return m.group(1).strip() if m else None

def _kv_top(text: str, key: str) -> str | None:
    m = re.search(rf"(?mi)^\s*{re.escape(key)}\s*:\s*['\"]?([^#\r\n'\"]+)['\"]?\s*(?:#.*)?$", text)
    return m.group(1).strip() if m else None

def _name_version(text: str, root: Path) -> tuple[str, str]:
    return (_kv_top(text, "name") or root.name, _kv_top(text, "version") or "0.0.0")

def _flavor(text: str) -> str:
    svc = _extract_block(text, "service")
    fl = (_kv_from_block(svc, "flavor") or "").lower().strip()
    return "db-mariadb" if fl in ("mariadb", "mysql", "db-mariadb", "db_mysql") else "python-app"

def _ecr_root(text: str) -> str | None:
    """
    Try env → explicit top-level ECR: → any ECR host in the file.
    """
    for var in ("ECR", "ECR_URL", "AWS_ECR", "AWS_ECR_URL"):
        v = os.getenv(var)
        if v and v.strip():
            return v.strip().rstrip("/")

    m_explicit = re.search(r'(?mi)^\s*ECR\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
    if m_explicit:
        return m_explicit.group(1).strip().rstrip("/")

    m_priv = re.search(r'([0-9]{12}\.dkr\.ecr\.[a-z0-9-]+)\.amazonaws\.com', text, re.I)
    if m_priv:
        return m_priv.group(0).strip().rstrip("/")

    m_pub = re.search(r'(public\.ecr\.aws/[A-Za-z0-9-]+)', text, re.I)
    if m_pub:
        return m_pub.group(1).strip().rstrip("/")

    return None

def _image_repository(text: str, service_name: str) -> str | None:
    """
    Prefer an explicit image.repository from config.yaml.
    If missing, return None (caller may fall back to ECR/name).
    """
    img_blk = _extract_block(text, "image")
    repo = _kv_from_block(img_blk, "repository")
    if repo:
        return repo.strip()
    m = re.search(r'(?mi)^\s*image\.repository\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
    if m:
        return m.group(1).strip()
    return None

def _db_env_values(text: str, env_name: str) -> tuple[str, str | None, str | None, str | None]:
    envs = _extract_block(text, "environments")
    blk = ""
    if envs:
        m = re.search(rf"(?ms)^\s*{re.escape(env_name)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", envs)
        blk = m.group("blk") if m else ""
    root_pw = _kv_from_block(blk, "root_pw")
    if not root_pw:
        dev = _extract_block(text, "dev")
        if dev:
            root = _extract_block(dev, "root")
            root_pw = _kv_from_block(root, "password") or root_pw
    db_blk  = _extract_block(blk, "database")
    db_name = _kv_from_block(db_blk, "name")
    db_user = _kv_from_block(db_blk, "user")
    db_pw   = _kv_from_block(db_blk, "password")
    return (root_pw or "Password1"), db_name, db_user, db_pw


def _have(cmd: str) -> bool:
    return shutil.which(cmd) is not None

def _ensure_namespace(ns: str) -> int:
    if not _have("kubectl"):
        console.warn("kubectl not found on PATH; relying on Helm to create namespace.")
        return 0
    cmd = f"kubectl get ns {shlex.quote(ns)} >/dev/null 2>&1 || kubectl create namespace {shlex.quote(ns)}"
    return console.run(cmd)

def _adopt_for_helm(ns: str, kind: str, name: str, release_name: str) -> int:
    """
    Add Helm ownership labels/annotations so Helm can adopt an existing resource.
    """
    if not _have("kubectl"):
        return 0
    rc = 0
    rc |= console.run(f"kubectl -n {shlex.quote(ns)} label {kind} {shlex.quote(name)} app.kubernetes.io/managed-by=Helm --overwrite")
    rc |= console.run(
        "kubectl -n {ns} annotate {kind} {name} "
        "meta.helm.sh/release-name={rel} meta.helm.sh/release-namespace={ns} --overwrite"
        .format(ns=shlex.quote(ns), kind=kind, name=shlex.quote(name), rel=shlex.quote(release_name))
    )
    return rc



def _write_dockerconfigjson_secret_yaml(*, ns: str, name: str, registry: str, username: str, password: str) -> str:
    """
    Create a Secret manifest (YAML text) of type kubernetes.io/dockerconfigjson
    without exposing the password in the command line.
    """
    auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    cfg = {
        "auths": {
            registry: {
                "username": username,
                "password": password,
                "email": "none",
                "auth": auth,
            }
        }
    }
    cfg_b64 = base64.b64encode(json.dumps(cfg).encode("utf-8")).decode("ascii")
    return (
        "apiVersion: v1\n"
        "kind: Secret\n"
        "type: kubernetes.io/dockerconfigjson\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {ns}\n"
        "data:\n"
        f"  .dockerconfigjson: {cfg_b64}\n"
    )

def _ensure_ecr_pull_secret(ns: str, secret_name: str, registry_host: str, password: str) -> int:
    if not _have("kubectl"):
        console.error("kubectl is required to create image pull secrets.")
        return 127
    yaml_text = _write_dockerconfigjson_secret_yaml(
        ns=ns, name=secret_name, registry=registry_host, username="AWS", password=password
    )
    tmp = None
    try:
        tmp = tempfile.NamedTemporaryFile(prefix="plsr-ecr-", suffix=".yaml", delete=False)
        tmp.write(yaml_text.encode("utf-8"))
        tmp.flush()
        tmp.close()
        return console.run(["kubectl", "apply", "-f", tmp.name])
    finally:
        try:
            if tmp and tmp.name:
                os.unlink(tmp.name)
        except Exception:
            pass


def _apply_opaque_secret(ns: str, name: str, kv: dict[str, str]) -> int:
    """
    Create/refresh an Opaque Secret from a key/value map without logging values.
    """
    if not _have("kubectl"):
        console.error("kubectl is required to create secrets.")
        return 127
    lines = [
        "apiVersion: v1",
        "kind: Secret",
        f"metadata:\n  name: {name}\n  namespace: {ns}",
        "type: Opaque",
        "data:",
    ]
    for k, v in (kv or {}).items():
        enc = base64.b64encode((v or "").encode("utf-8")).decode("ascii")
        lines.append(f"  {k}: {enc}")
    yaml_text = "\n".join(lines) + "\n"

    tmp = None
    try:
        tmp = tempfile.NamedTemporaryFile(prefix="plsr-sm-", suffix=".yaml", delete=False)
        tmp.write(yaml_text.encode("utf-8"))
        tmp.flush()
        tmp.close()
        return console.run(["kubectl", "apply", "-f", tmp.name])
    finally:
        try:
            if tmp and tmp.name:
                os.unlink(tmp.name)
        except Exception:
            pass


def _rand_suffix() -> str:
    return base64.urlsafe_b64encode(os.urandom(4)).decode("ascii").rstrip("=").lower()

def _write_pullcheck_pod_yaml(*, ns: str, name: str, image: str, secret: str | None) -> str:
    """
    Pod spec that attempts to pull <image>. We don't require the container to fully
    run to "Ready"; success is determined by imageID being populated.
    """
    lines = [
        "apiVersion: v1",
        "kind: Pod",
        "metadata:",
        f"  name: {name}",
        f"  namespace: {ns}",
        "  labels:",
        "    app.kubernetes.io/name: plsr-pullcheck",
        "spec:",
        "  restartPolicy: Never",
    ]
    if secret:
        lines += ["  imagePullSecrets:", f"  - name: {secret}"]
    lines += [
        "  containers:",
        "  - name: pullcheck",
        f"    image: {image}",
        "    imagePullPolicy: Always",
        "    command: [\"/bin/sh\", \"-c\"]",
        "    args: [\"sleep 5\"]",
    ]
    return "\n".join(lines) + "\n"

def _pod_status_json(ns: str, pod: str) -> dict | None:
    """
    Return pod JSON or None.
    """
    cmd = ["kubectl", "-n", ns, "get", "pod", pod, "-o", "json"]
    console.command(cmd)
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        return None
    try:
        return json.loads(res.stdout or "{}")
    except Exception:
        return None

def _delete_pod(ns: str, pod: str) -> None:
    cmd = ["kubectl", "-n", ns, "delete", "pod", pod, "--ignore-not-found", "--wait=false"]
    console.command(cmd)
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def _verify_namespace_can_pull_image(*, ns: str, image: str, release_name: str, secret: str | None) -> int:
    """
    Launch a short-lived pod that uses <image>. We consider it a success as soon as
    the Kubelet reports a non-empty imageID for the container. If we encounter
    ErrImagePull/ImagePullBackOff/etc., we fail and abort Helm release.
    """
    if not _have("kubectl"):
        console.error("kubectl is required for image pull preflight.")
        return 127

    pod_name = f"pullcheck-{release_name}-{_rand_suffix()}"
    yaml_text = _write_pullcheck_pod_yaml(ns=ns, name=pod_name, image=image, secret=secret)

    tmp = None
    try:
        tmp = tempfile.NamedTemporaryFile(prefix="plsr-pullcheck-", suffix=".yaml", delete=False)
        tmp.write(yaml_text.encode("utf-8"))
        tmp.flush()
        tmp.close()

        rc = console.run(["kubectl", "apply", "-f", tmp.name])
        if rc != 0:
            console.error("Failed to create pullcheck pod.")
            return rc

        timeout_s = int(os.getenv("plsr_ECR_PULLCHECK_TIMEOUT", "120"))
        deadline = time.time() + max(10, timeout_s)
        last_reason = ""
        last_message = ""
        while time.time() < deadline:
            obj = _pod_status_json(ns, pod_name)
            if not obj:
                time.sleep(2)
                continue
            statuses = (obj.get("status") or {}).get("containerStatuses") or []
            if statuses:
                st = statuses[0] or {}
                image_id = st.get("imageID") or ""
                if image_id:
                    console.success("Cluster verified: image pulled successfully.")
                    return 0
                waiting = (st.get("state") or {}).get("waiting") or {}
                reason = (waiting.get("reason") or "").strip()
                message = (waiting.get("message") or "").strip()
                if reason in {"ErrImagePull", "ImagePullBackOff", "RegistryUnavailable", "InvalidImageName", "Unauthorized", "Forbidden"}:
                    last_reason, last_message = reason, message
                    break
            time.sleep(2)

        if last_reason:
            console.error(f"Image pull preflight failed: {last_reason}: {last_message}")
        else:
            console.error("Image pull preflight did not succeed before timeout.")
            console.tip("Check node egress, cluster DNS, and image/credentials. Inspect events with: kubectl describe pod …")
        return 1
    finally:
        try:
            if tmp and tmp.name:
                os.unlink(tmp.name)
        except Exception:
            pass
        _delete_pod(ns, pod_name)


def _preflight_ecr_for_k8s_pull(*, namespace: str, image_repo: str, tag: str, release_name: str) -> tuple[list[str], int]:
    """
    If using ECR, ensure:
      • (private ECR) AWS creds/region are valid (sts get-caller-identity),
        obtain ECR token, create docker-registry Secret in the namespace,
      • Then launch a pullcheck Pod in the *cluster* namespace using the target image
        to verify the cluster can actually pull it.
      • (public ECR) run the same pullcheck without a secret.

    Returns (extra_helm_sets, exit_code) — non-zero exit aborts Helm release.
    """
    image_ref = f"{image_repo}:{tag}"
    parts = parse_ecr_image_ref(image_ref)

    if not parts:
        console.warn("Image does not look like an ECR reference; skipping ECR-specific preflight.")
        return [], 0

    host = str(parts["host"])
    region = str(parts.get("region") or "")
    is_private = bool(parts.get("private", False))
    secret_name = f"ecr-pull-{release_name}" if is_private else None

    console.section("ECR connectivity (cluster preflight)")
    console.info(f"Registry: {host}")
    if region:
        console.info(f"Region:   {region}")
    if secret_name:
        console.info(f"Secret:   {secret_name}")

    if is_private:
        if not region:
            console.error("Could not determine AWS region from ECR host.")
            return [], 1

        rc, _ = aws_sts_identity(region)
        if rc != 0:
            console.error("AWS identity check failed; cannot verify cluster pull.")
            return [], rc

        rc, password = ecr_get_login_password(region)
        if rc != 0 or not password:
            console.error("Failed to get ECR login password; cannot verify cluster pull.")
            return [], rc or 1

        rc = _ensure_ecr_pull_secret(namespace, secret_name, host, password)
        if rc != 0:
            console.error("Failed to provision image pull secret in the cluster namespace.")
            return [], rc

        _adopt_for_helm(namespace, "secret", secret_name, release_name)

        rc = _verify_namespace_can_pull_image(ns=namespace, image=image_ref, release_name=release_name, secret=secret_name)
        if rc != 0:
            return [], rc

        return (["--set", f"imagePullSecrets[0]={secret_name}"], 0)


    console.info("Public ECR image detected; verifying pull without a secret…")
    rc = _verify_namespace_can_pull_image(ns=namespace, image=image_ref, release_name=release_name, secret=None)
    if rc != 0:
        return [], rc
    return [], 0


def release(env_name: str) -> int:
    """
    Deploy using the central plsr Helm chart to namespace <env>.
    Reads image/values from the microservice's config.yaml.
    Creates namespace + secrets if needed. Uses --wait --atomic.

    Before deployment, performs a *cluster-side* image pull preflight so that
    we don't proceed if the namespace cannot pull from ECR. This prevents pods
    failing later with ImagePullBackOff.
    """
    if not env_name:
        console.error("Environment name is required. Use: ctl.sh <env> helm release")
        return 2

    if not _have("helm"):
        console.error("Helm is not installed or not on PATH.")
        return 127

    app_root = _detect_app_root()
    cfg_text = _read_cfg_text(app_root)
    if not cfg_text:
        console.error(f"No config.yaml found under {app_root}")
        return 2

    plsr_root = Path(__file__).resolve().parent.parent
    helm_dir = Path(os.getenv("plsr_HELM_DIR", str(plsr_root / "helm"))).resolve()
    if not helm_dir.is_dir():
        console.error(f"No Helm chart directory at: {helm_dir}")
        console.tip("Set plsr_HELM_DIR to override or ensure plsr/helm exists.")
        return 2

    name, ver = _name_version(cfg_text, app_root)
    flavor = _flavor(cfg_text)

    image_repo = _image_repository(cfg_text, name)
    if not image_repo:
        ecr = _ecr_root(cfg_text)
        if ecr:
            image_repo = f"{ecr.rstrip('/')}/{name}"
        else:
            console.error("Could not determine image repository.")
            console.tip("Add 'image.repository: <registry>/<repo>' to config.yaml, or top-level 'ECR: <registry>'.")
            return 2

    namespace = f"{env_name}"
    root_pw, db_name, db_user, db_pw = _db_env_values(cfg_text, env_name)

    if _ensure_namespace(namespace) != 0:
        return 1

    release_name = name
    root_secret = f"{name}-root-{env_name}"

    _apply_opaque_secret(namespace, root_secret, {"ROOT_DB_PW": root_pw})
    _adopt_for_helm(namespace, "secret", root_secret, release_name)


    extra_sets, rc = _preflight_ecr_for_k8s_pull(
        namespace=namespace, image_repo=image_repo, tag=ver, release_name=release_name
    )
    if rc != 0:
        console.error("Aborting Helm release: cluster cannot pull the image from the registry (preflight failed).")
        return rc

    timeout = os.getenv("plsr_HELM_TIMEOUT", "5m")
    cmd: list[str] = [
        "helm", "upgrade", "--install", release_name, str(helm_dir),
        "-n", namespace, "--create-namespace",
        "--wait", "--atomic", "--timeout", timeout,
        "--set", f"fullnameOverride={name}",
        "--set", f"flavor={flavor}",
        "--set", f"image.repository={image_repo}",
        "--set", f"image.tag={ver}",
        "--set", "persistentVolume.enabled=true",
        "--set-string", "persistentVolume.size=10Gi",
    ]

    cmd.extend(extra_sets)

    if flavor == "db-mariadb":
        cmd += [
            "--set", "service.port=3306",
            "--set-string", "service.name=mysql",
            "--set-string", f"auth.secretName={root_secret}",
            "--set", f"auth.rootPassword={root_pw}",
        ]
        if db_name:
            cmd += ["--set", f"auth.database={db_name}"]
        if db_user:
            cmd += ["--set", f"auth.username={db_user}"]
        if db_pw:
            cmd += ["--set", f"auth.password={db_pw}"]

    console.section("Helm Release")
    console.info(f"Env:        {env_name}")
    console.info(f"Namespace:  {namespace}")
    console.info(f"Release:    {release_name}")
    console.info(f"Flavor:     {flavor}")
    console.info(f"Chart:      {helm_dir}")
    console.command(cmd, cwd=str(helm_dir))
    return console.run(cmd, cwd=str(helm_dir))
