from __future__ import annotations

import os
import re
import sys
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .console import console
from .aws import (
    preflight_aws_and_ecr,
    ecr_login,
    parse_ecr_image_ref,
    ecr_image_exists,
    ecr_ensure_repo,
    ecr_delete_image_by_tag,
)

def _find_app_root_for_config() -> Path:
    env_root = os.getenv("APP_ROOT")
    if env_root:
        p = Path(env_root).resolve()
        if (p / "config.yaml").is_file():
            return p

    cur = Path.cwd().resolve()
    for p in (cur, *cur.parents):
        if (p / "config.yaml").is_file():
            return p
    return cur

def _read_text_config(app_root: Path) -> str:
    cfg = app_root / "config.yaml"
    if not cfg.is_file():
        return ""
    try:
        return cfg.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def _kv_top(text: str, key: str) -> Optional[str]:
    m = re.search(rf"(?mi)^\s*{re.escape(key)}\s*:\s*['\"]?([^#\r\n'\"]+)['\"]?\s*(?:#.*)?$", text)
    return m.group(1).strip() if m else None

def _extract_block(text: str, section: str) -> str:
    m = re.search(rf"(?ms)^\s*{re.escape(section)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", text)
    return m.group("blk") if m else ""

def _kv_from_block(block: str, key: str) -> Optional[str]:
    m = re.search(rf"(?mi)^\s*{re.escape(key)}\s*:\s*['\"]?([^#\r\n'\"]+)['\"]?\s*(?:#.*)?$", block)
    return m.group(1).strip() if m else None

def _read_fields_from_config_yaml(app_root: Path) -> tuple[str | None, str | None, str | None, str]:
    """
    Return (name, version, ecr_root, flavor).
    ecr_root can be explicit top-level ECR: ... or discovered from references.

    EXPLICIT-ONLY flavor detection: only mark as 'db-mariadb' when service.flavor
    is explicitly mariadb/mysql. Otherwise treat as 'python-app' (even if the file
    mentions mysql/mariadb, e.g., in DATABASE_URL).
    """
    text = _read_text_config(app_root)
    if not text:
        return None, None, None, "python-app"

    m_name = re.search(r'(?m)^\s*name\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
    m_ver  = re.search(r'(?m)^\s*version\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
    name = m_name.group(1).strip() if m_name else None
    ver  = m_ver.group(1).strip() if m_ver else None

    svc = _extract_block(text, "service")
    fl  = (_kv_from_block(svc, "flavor") or "").lower().strip()
    flavor = "db-mariadb" if fl in ("mariadb", "mysql", "db-mariadb", "db_mysql") else "python-app"

    m_ecr  = re.search(r'(?mi)^\s*ECR\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
    ecr = m_ecr.group(1).strip().rstrip("/") if m_ecr else None
    if not ecr:
        env = (
            os.getenv("ECR")
            or os.getenv("ECR_URL")
            or os.getenv("AWS_ECR")
            or os.getenv("AWS_ECR_URL")
        )
        if env:
            ecr = env.strip().rstrip("/")

    if not ecr:
        m_priv = re.search(r'([0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com)', text)
        if m_priv:
            ecr = m_priv.group(1).strip().rstrip("/")
        else:
            m_pub = re.search(r'(public\.ecr\.aws/[A-Za-z0-9-]+)', text)
            if m_pub:
                ecr = m_pub.group(1).strip().rstrip("/")

    return name, ver, ecr, flavor

def _read_dockerfile_from_config_yaml(app_root: Path) -> str | None:
    text = _read_text_config(app_root)
    dockerfile = None
    if text:
        m_df = re.search(r'(?mi)^\s*(?:build\.)?dockerfile\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
        if m_df:
            dockerfile = m_df.group(1).strip()
    if not dockerfile:
        candidate = app_root / "docker" / "Dockerfile"
        if candidate.is_file():
            dockerfile = str(candidate.relative_to(app_root))
    return dockerfile

def _read_build_args_map(app_root: Path) -> Dict[str, str]:
    """
    Parse build.args: mapping into a dict of strings.
    """
    text = _read_text_config(app_root)
    args_block = _extract_block(text, "build")
    m = re.search(r"(?ms)^\s*args\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", args_block)
    blk = m.group("blk") if m else ""
    out: Dict[str, str] = {}
    for line in (blk.splitlines() if blk else []):
        m2 = re.match(r"^[ \t]+([A-Za-z_][A-Za-z0-9_]*)\s*:\s*['\"]?([^'\"]+)['\"]?\s*(?:#.*)?$", line)
        if m2:
            out[m2.group(1)] = m2.group(2).strip()
    return out

def _read_targets_list(app_root: Path) -> List[str]:
    """
    Read image.targets: list (e.g., ['local','runtime']).
    """
    text = _read_text_config(app_root)
    img_blk = _extract_block(text, "image")
    m = re.search(r"(?ms)^\s*targets\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", img_blk)
    blk = m.group("blk") if m else ""
    items: List[str] = []
    for line in (blk.splitlines() if blk else []):
        m2 = re.match(r"^[ \t]*-\s*([A-Za-z0-9_.-]+)\s*$", line)
        if m2:
            items.append(m2.group(1).strip())
    return items

def _compose_image_ref(name: str, version: str, ecr: str) -> str:
    return f"{ecr.rstrip('/')}/{name}:{version}"

def _ensure_docker_buildx_available() -> bool:
    if shutil.which("docker") is None:
        console.error("docker is not installed or not on PATH.")
        return False
    try:
        subprocess.run(["docker", "buildx", "version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except Exception:
        console.error("docker buildx not available. Install Buildx or update Docker Desktop/Engine.")
        return False

def _select_target_for_db(app_root: Path) -> Optional[str]:
    targets = _read_targets_list(app_root)
    if "runtime" in targets:
        return "runtime"
    if "local" in targets:
        return "local"
    return None

def _confirm(prompt: str, default_no: bool = True) -> bool:
    """
    Ask a yes/no in the terminal. Returns True for yes.
    Env bypass:
      PLSR_AUTO_REBUILD=1 → assume yes
      PLSR_AUTO_REBUILD=0 → assume no
    """
    env = os.getenv("PLSR_AUTO_REBUILD")
    if env is not None:
        return env.strip() in ("1", "true", "yes", "y", "Y")

    suffix = " [y/N] " if default_no else " [Y/n] "
    try:
        ans = input(prompt + suffix).strip().lower()
    except EOFError:
        ans = ""
    if not ans:
        return not default_no
    return ans in ("y", "yes")

def _db_build_platforms() -> str:
    """
    Decide platforms for DB image builds.
    Default: multi-arch so ECR pulls work on both x86_64 and ARM hosts.
    Overrides:
      - PLSR_DB_PLATFORMS
      - PLSR_PLATFORMS
      - DOCKER_PLATFORMS
    """
    val = (
        os.getenv("PLSR_DB_PLATFORMS")
        or os.getenv("PLSR_PLATFORMS")
        or os.getenv("DOCKER_PLATFORMS")
    )
    return (val or "linux/amd64,linux/arm64").strip()


def _sanitize_name(n: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", n)[:63] or "plsr-service"

def _container_exists(name: str) -> bool:
    res = subprocess.run(
        ["docker", "inspect", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return res.returncode == 0

def _container_running(name: str) -> bool:
    res = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return False
    return (res.stdout or "").strip().lower() == "true"

def _stop_local_container_and_delete_data_if_exists(app_root: Path, cname: str) -> int:
    """
    Reuse plsr's stop flow (which also deletes the mounted DB data dir by default)
    if the container currently exists (running or stopped). No-op otherwise.

    We call `python -m plsr.bootstrap docker stop <env> --name <cname>`
    so we can reuse the same logic without importing run_local (avoids circular imports).
    If PLSR_ENV is not set, default to 'local'.
    """
    if shutil.which("docker") is None:
        return 0
    if not _container_exists(cname):
        return 0

    env_name = os.getenv("PLSR_ENV") or "local"

    console.info(f"Stopping & cleaning local DB container before rebuild: {cname} (env: {env_name})")
    cmd = [sys.executable, "-m", "plsr.bootstrap", "docker", "stop", env_name, "--name", cname]
    env = os.environ.copy()
    env["APP_ROOT"] = str(app_root)
    env["PLSR_ENV"] = env_name
    return console.run(cmd, cwd=str(app_root), env=env)

def _read_dev_compose_container_name(app_root: Path) -> Optional[str]:
    text = _read_text_config(app_root)
    dev_blk = _extract_block(text, "dev")
    if not dev_blk:
        return None
    m = re.search(r"(?ms)^\s*compose\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", dev_blk)
    comp_blk = m.group("blk") if m else ""
    cname = _kv_from_block(comp_blk, "container_name")
    return cname.strip() if cname else None

def _db_default_container_name(app_root: Path, service_name: str) -> str:
    return _sanitize_name(_read_dev_compose_container_name(app_root) or service_name)


def _buildx_build_and_maybe_push(
    *,
    app_root: Path,
    image_ref: str,
    dockerfile: Optional[str],
    platform: str = "linux/amd64",
    target: Optional[str] = None,
    no_cache: bool = False,
    build_args_map: Optional[Dict[str, str]] = None,
    push: bool = False,
    pre_pull: bool = True,
) -> int:
    if not _ensure_docker_buildx_available():
        return 3

    ecr_root = image_ref.split("/", 1)[0] if "/" in image_ref else None
    rc = preflight_aws_and_ecr(
        app_root=app_root,
        dockerfile=dockerfile,
        build_args=[f"{k}={v}" for k, v in (build_args_map or {}).items()],
        dest_ecr=ecr_root,
        need_push=bool(push),
        pre_pull=bool(pre_pull),
    )
    if rc != 0:
        return rc

    cmd: List[str] = ["docker", "buildx", "build", "--platform", platform, "-t", image_ref]
    if dockerfile:
        cmd.extend(["-f", dockerfile])
    if target:
        cmd.extend(["--target", target])
    if no_cache:
        cmd.append("--no-cache")
    for k, v in (build_args_map or {}).items():
        cmd.extend(["--build-arg", f"{k}={v}"])
    if push:
        cmd.append("--push")
    else:
        cmd.append("--load")
    cmd.append(".")

    console.section("Docker Build")
    console.info(f"Image:  {image_ref}")
    if target:
        console.info(f"Target: {target}")
    return console.run(cmd, cwd=str(app_root))

def ensure_db_image_in_ecr(mode: str = "build") -> Tuple[str, int]:
    """
    Ensure the DB microservice image exists (and optionally rebuild) in ECR.

    Behavior:
      - If image tag <ECR>/<name>:<version> is missing → build & push.
      - If it exists → prompt to rebuild. If yes:
          • Stop/remove the local DB container(s) and delete the mounted data dir,
          • Delete tag from ECR,
          • Build (multi-arch by default) and push.
      - If prompt answer is 'no' → skip build and return success.

    Returns: (image_ref, exit_code)
    """
    app_root = _find_app_root_for_config()
    name, ver, ecr, flavor = _read_fields_from_config_yaml(app_root)

    if not name or not ver:
        console.error("config.yaml must contain top-level 'name' and 'version'.")
        return "", 2
    if flavor != "db-mariadb":
        return "", 0
    if not ecr:
        console.error("ECR root is required for DB image enforcement. Add 'ECR: <registry-root>' to config.yaml.")
        return "", 2

    image_ref = _compose_image_ref(name, ver, ecr)
    env_name = os.getenv("PLSR_ENV") or "local"
    cname_db  = _db_default_container_name(app_root, name)
    cname_env = _sanitize_name(f"{name}-{env_name}")

    parts = parse_ecr_image_ref(image_ref)
    if not parts or not parts.get("private"):
        console.warn("Non-private ECR detected or unable to parse ECR host; skipping ECR enforcement.")
        return image_ref, 0

    host   = parts["host"]
    region = parts["region"]
    acct   = parts["account"]
    repo   = parts["repository"]
    tag    = parts["tag"]

    rc = ecr_login(host, region)
    if rc != 0:
        return image_ref, rc

    rc = ecr_ensure_repo(host, region, repo, account=acct)
    if rc != 0:
        return image_ref, rc

    exists, _ = ecr_image_exists(host, region, repo, tag, account=acct)

    dockerfile = _read_dockerfile_from_config_yaml(app_root)
    build_args_map = _read_build_args_map(app_root)
    target = _select_target_for_db(app_root)
    platforms = _db_build_platforms()

    if not exists:
        console.info(f"ECR: '{image_ref}' does not exist. Building and pushing…")
        rc = _buildx_build_and_maybe_push(
            app_root=app_root,
            image_ref=image_ref,
            dockerfile=dockerfile,
            platform=platforms,
            target=target,
            no_cache=False,
            build_args_map=build_args_map,
            push=True,
            pre_pull=True,
        )
        return image_ref, rc

    prompt_text = f"ECR image already exists: {image_ref}. Rebuild from scratch?"
    rebuild = _confirm(prompt_text, default_no=True)
    if rebuild:
        stop_rc = _stop_local_container_and_delete_data_if_exists(app_root, cname_db)
        if cname_env != cname_db:
            stop_rc2 = _stop_local_container_and_delete_data_if_exists(app_root, cname_env)
            stop_rc = stop_rc or stop_rc2
        if stop_rc != 0:
            console.warn("Local stop/cleanup reported a non-zero exit; continuing with rebuild.")

        console.info("Deleting existing tag from ECR…")
        rc = ecr_delete_image_by_tag(host, region, repo, tag, account=acct)
        if rc != 0:
            return image_ref, rc
        console.info("Rebuilding (no cache) and pushing…")
        rc = _buildx_build_and_maybe_push(
            app_root=app_root,
            image_ref=image_ref,
            dockerfile=dockerfile,
            platform=platforms,
            target=target,
            no_cache=True,
            build_args_map=build_args_map,
            push=True,
            pre_pull=True,
        )
        return image_ref, rc

    if mode == "build":
        console.info("Skipping build (image exists).")
    else:
        console.info("Proceeding without rebuild (image exists).")
    return image_ref, 0


def docker_build_from_config(
    context: str = ".",
    dockerfile: str | None = None,
    platform: str = "linux/amd64",
    push: bool = False,
    target: str | None = None,
    no_cache: bool = False,
    build_args: List[str] | None = None,
    labels: List[str] | None = None,
    preflight: bool = True,
    pre_pull: bool = True,
) -> int:
    """
    Replacement for the old CLI builder. For DB microservices, this enforces the ECR
    existence-check + prompt + (re)build + push flow. For non-DB services, behaves
    like the previous implementation.
    """
    app_root = _find_app_root_for_config()
    name, ver, ecr, flavor = _read_fields_from_config_yaml(app_root)
    if not name or not ver:
        console.error("Missing required fields in config.yaml. Need top-level keys 'name' and 'version'.")
        console.tip(f"Searched: {app_root / 'config.yaml'}")
        return 2

    if flavor == "db-mariadb":
        _, rc = ensure_db_image_in_ecr(mode="build")
        return rc

    if not ecr:
        console.error("Could not locate ECR registry in config/env.")
        console.tip("Add 'ECR: <registry>' or include an ECR-based image (e.g., image.base/BASE_IMAGE).")
        console.tip(f"Searched: {app_root / 'config.yaml'}")
        return 2

    image_ref = _compose_image_ref(name, ver, ecr)
    if not dockerfile:
        dockerfile = _read_dockerfile_from_config_yaml(app_root)

    build_args_map = {}
    for ba in (build_args or []):
        if "=" in ba:
            k, v = ba.split("=", 1)
            k = k.strip()
            if k:
                build_args_map[k] = v

    rc = _buildx_build_and_maybe_push(
        app_root=app_root,
        image_ref=image_ref,
        dockerfile=dockerfile,
        platform=platform,
        target=target,
        no_cache=no_cache,
        build_args_map=build_args_map,
        push=bool(push),
        pre_pull=bool(pre_pull),
    )
    return rc

def print_image_tag_from_config() -> int:
    app_root = _find_app_root_for_config()
    name, ver, ecr, _ = _read_fields_from_config_yaml(app_root)
    if not name or not ver:
        console.error("Missing required fields in config.yaml. Need top-level keys 'name' and 'version'.")
        console.tip(f"Searched: {app_root / 'config.yaml'}")
        return 2
    if not ecr:
        console.error("Could not locate ECR registry in config/env.")
        console.tip("Add 'ECR: <registry>' or include an ECR-based image reference.")
        console.tip(f"Searched: {app_root / 'config.yaml'}")
        return 2
    print(_compose_image_ref(name, ver, ecr))
    return 0
