from __future__ import annotations

import os
import re
import shutil
import subprocess
import json
import platform as _platform
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from ..core.console import console

_PRIV_ECR_HOST = re.compile(r"(?P<host>(?P<acct>\d{12})\.dkr\.ecr\.(?P<region>[a-z0-9-]+)\.amazonaws\.com)")
_PUB_ECR_HOST = re.compile(r"(?:^|/)(public\.ecr\.aws)(?:/|$)")
_VAR_BRACE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_VAR_SIMPLE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


def _which(cmd: str) -> Optional[str]:
    return shutil.which(cmd)


def _require(cmd: str, friendly: str) -> bool:
    if _which(cmd) is None:
        console.error(f"{friendly} not found on PATH.")
        return False
    return True


def _aws_env(extra: Optional[dict] = None) -> dict:
    env = os.environ.copy()
    env["AWS_PAGER"] = ""
    if extra:
        env.update(extra)
    return env


def _default_region(fallback: Optional[str] = None) -> Optional[str]:
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or fallback


def _extract_ecr_host_region(s: str) -> Optional[Tuple[str, str]]:
    m = _PRIV_ECR_HOST.search(s)
    if not m:
        return None
    return m.group("host"), m.group("region")


def _is_public_ecr(s: str) -> bool:
    return _PUB_ECR_HOST.search(s) is not None


def aws_sts_identity(region: Optional[str]) -> Tuple[int, str]:
    if not _require("aws", "aws CLI"):
        return 127, "aws CLI missing"
    cmd = ["aws", "--no-cli-pager", "sts", "get-caller-identity"]
    if region:
        cmd += ["--region", region]
    console.command(cmd)
    res = subprocess.run(cmd, capture_output=True, text=True, env=_aws_env())
    out = res.stdout.strip() if res.stdout else res.stderr.strip()
    if res.returncode == 0:
        console.success(f"AWS identity OK: {out}")
    else:
        console.error(f"AWS identity check failed: {out}")
    return res.returncode, out


def ecr_login(registry_host: str, region: str) -> int:
    if not _require("aws", "aws CLI"):
        return 127
    if not _require("docker", "docker"):
        return 127

    console.info(f"Logging into ECR: {registry_host} (region: {region})")
    try:
        pw = subprocess.check_output(
            ["aws", "--no-cli-pager", "ecr", "get-login-password", "--region", region],
            text=True,
            env=_aws_env(),
        )
    except subprocess.CalledProcessError as e:
        console.error("Failed to get ECR login password.")
        console.tip("Ensure credentials & region are configured (AWS_PROFILE / AWS_REGION).")
        return e.returncode or 1

    proc = subprocess.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", registry_host],
        input=pw,
        text=True,
    )
    if proc.returncode == 0:
        console.success(f"ECR login OK: {registry_host}")
    else:
        console.error(f"ECR login failed for {registry_host} (exit {proc.returncode})")
    return int(proc.returncode or 0)


def ecr_get_login_password(region: str) -> Tuple[int, str]:
    if not _require("aws", "aws CLI"):
        return 127, ""
    try:
        pw = subprocess.check_output(
            ["aws", "--no-cli-pager", "ecr", "get-login-password", "--region", region],
            text=True,
            env=_aws_env(),
        )
        return 0, (pw or "").strip()
    except subprocess.CalledProcessError as e:
        err = (e.stderr or "").strip() or (getattr(e, "output", "") or "")
        return e.returncode or 1, str(err).strip()


def parse_ecr_image_ref(image_ref: str) -> Optional[Dict[str, str]]:
    if "@" in image_ref:
        if "/" in image_ref:
            host, rest = image_ref.split("/", 1)
            repo_part = rest.split("@", 1)[0]
            image_ref = f"{host}/{repo_part}"
        else:
            image_ref = image_ref.split("@", 1)[0]

    if "/" not in image_ref:
        return None
    host, rest = image_ref.split("/", 1)
    if ":" in rest:
        repository, tag = rest.rsplit(":", 1)
    else:
        repository, tag = rest, "latest"

    m = _PRIV_ECR_HOST.match(host)
    if m:
        return {
            "host": host,
            "region": m.group("region"),
            "account": m.group("acct"),
            "repository": repository,
            "tag": tag,
            "private": True,
        }

    if _PUB_ECR_HOST.search(host):
        return {
            "host": host,
            "region": "",
            "account": "",
            "repository": repository,
            "tag": tag,
            "private": False,
        }

    return None


def ecr_repo_exists(host: str, region: str, repository: str, *, account: Optional[str] = None) -> Tuple[bool, Optional[dict]]:
    if not _require("aws", "aws CLI"):
        return False, None
    cmd = ["aws", "--no-cli-pager", "ecr", "describe-repositories", "--repository-names", repository, "--region", region]
    if account:
        cmd += ["--registry-id", account]
    res = subprocess.run(cmd, capture_output=True, text=True, env=_aws_env())
    if res.returncode == 0:
        try:
            data = json.loads(res.stdout or "{}")
        except Exception:
            data = {}
        return True, data
    return False, None


def ecr_ensure_repo(host: str, region: str, repository: str, *, account: Optional[str] = None) -> int:
    ok, _ = ecr_repo_exists(host, region, repository, account=account)
    if ok:
        return 0
    console.info(f"Creating ECR repository '{repository}' in {region}…")
    cmd = ["aws", "--no-cli-pager", "ecr", "create-repository", "--repository-name", repository, "--region", region]
    if account:
        cmd += ["--registry-id", account]
    res = subprocess.run(cmd, env=_aws_env())
    return int(res.returncode or 0)


def ecr_image_exists(host: str, region: str, repository: str, tag: str, *, account: Optional[str] = None) -> Tuple[bool, Optional[dict]]:
    if not _require("aws", "aws CLI"):
        return False, None
    cmd = [
        "aws", "--no-cli-pager", "ecr", "describe-images",
        "--repository-name", repository, "--image-ids", f"imageTag={tag}",
        "--region", region, "--output", "json"
    ]
    if account:
        cmd += ["--registry-id", account]
    res = subprocess.run(cmd, capture_output=True, text=True, env=_aws_env())
    if res.returncode == 0:
        try:
            data = json.loads(res.stdout or "{}")
        except Exception:
            data = {}
        if isinstance(data, dict) and data.get("imageDetails"):
            return True, data
        return False, data
    return False, None


def ecr_delete_image_by_tag(host: str, region: str, repository: str, tag: str, *, account: Optional[str] = None) -> int:
    if not _require("aws", "aws CLI"):
        return 127
    cmd = [
        "aws", "--no-cli-pager", "ecr", "batch-delete-image",
        "--repository-name", repository, "--image-ids", f"imageTag={tag}",
        "--region", region
    ]
    if account:
        cmd += ["--registry-id", account]
    res = subprocess.run(cmd, env=_aws_env())
    return int(res.returncode or 0)


def _resolve_vars(template: str, mapping: Dict[str, str]) -> str:
    def br(m):
        key = m.group(1)
        return mapping.get(key, os.getenv(key, ""))
    def sm(m):
        key = m.group(1)
        return mapping.get(key, os.getenv(key, ""))
    out = _VAR_BRACE.sub(br, template)
    out = _VAR_SIMPLE.sub(sm, out)
    return out


def _build_arg_map(build_args: Iterable[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for item in build_args or []:
        if "=" in item:
            k, v = item.split("=", 1)
            k = k.strip()
            if k:
                m[k] = v
    return m


def parse_dockerfile_base_images(dockerfile_path: Path, build_args: Iterable[str]) -> List[str]:
    if not dockerfile_path.is_file():
        return []
    text = dockerfile_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    global_args: Dict[str, str] = _build_arg_map(build_args)
    current_args = dict(global_args)
    bases: List[str] = []
    for raw in text:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.upper().startswith("ARG "):
            arg_spec = line[4:].strip()
            if "=" in arg_spec:
                k, v = arg_spec.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k not in global_args:
                    current_args[k] = v
            else:
                k = arg_spec.strip()
                if k not in current_args:
                    current_args[k] = global_args.get(k, "")
            continue
        if line.upper().startswith("FROM "):
            rest = line[5:].strip()
            if rest.startswith("--platform="):
                parts = rest.split(maxsplit=1)
                rest = parts[1] if len(parts) > 1 else ""
            if not rest:
                continue
            parts = rest.split()
            image = parts[0]
            resolved = _resolve_vars(image, current_args).strip()
            if resolved and resolved.lower() != "scratch" and not resolved.startswith("--"):
                bases.append(resolved)
            continue
    seen = set()
    unique = []
    for b in bases:
        if b not in seen:
            unique.append(b)
            seen.add(b)
    return unique


def _collect_private_ecr_hosts(images: Iterable[str]) -> Dict[str, str]:
    hosts: Dict[str, str] = {}
    for img in images or []:
        ext = _extract_ecr_host_region(img)
        if ext:
            host, region = ext
            hosts[host] = region
    return hosts


def preflight_aws_and_ecr(
    *,
    app_root: Path,
    dockerfile: Optional[str],
    build_args: List[str],
    dest_ecr: Optional[str],
    need_push: bool,
    pre_pull: bool = True,
) -> int:
    if os.getenv("PULSAR_SKIP_AWS_PREFLIGHT") == "1":
        console.warn("Skipping AWS/ECR preflight (PULSAR_SKIP_AWS_PREFLIGHT=1).")
        return 0

    if not _require("docker", "docker"):
        return 1
    if not _require("aws", "aws CLI"):
        return 1

    console.section("AWS/ECR preflight")

    dockerfile_path: Optional[Path] = None
    if dockerfile:
        dfp = Path(dockerfile)
        dockerfile_path = (app_root / dfp) if not dfp.is_absolute() else dfp
    else:
        for cand in ["Dockerfile", "docker/Dockerfile"]:
            p = app_root / cand
            if p.is_file():
                dockerfile_path = p
                break

    base_images: List[str] = []
    if dockerfile_path:
        base_images = parse_dockerfile_base_images(dockerfile_path, build_args)
        if base_images:
            console.info(f"Discovered base images: {', '.join(base_images)}")
        else:
            console.info("No base images discovered (or Dockerfile not found).")

    host_regions = _collect_private_ecr_hosts(base_images)

    if dest_ecr and not _is_public_ecr(dest_ecr):
        ext = _extract_ecr_host_region(dest_ecr)
        if ext:
            h, r = ext
            host_regions[h] = r

    sts_region = None
    if dest_ecr:
        ext = _extract_ecr_host_region(dest_ecr)
        if ext:
            sts_region = ext[1]
    if not sts_region and host_regions:
        sts_region = next(iter(host_regions.values()))
    sts_region = _default_region(sts_region)

    code, _ = aws_sts_identity(sts_region)
    if code != 0:
        return code

    for host, region in host_regions.items():
        rc = ecr_login(host, region)
        if rc != 0:
            return rc

    if pre_pull and base_images:
        for img in base_images:
            if _extract_ecr_host_region(img):
                console.info(f"Pre-pulling base image: {img}")
                rc = console.run(["docker", "pull", img], cwd=str(app_root))
                if rc != 0:
                    return rc

    console.success("AWS/ECR preflight checks passed.")
    return 0


def ensure_image_pullable(image_ref: str, *, allow_skip: bool = False, force_pull: bool = False) -> int:
    if not _require("docker", "docker"):
        return 1

    if allow_skip:
        console.warn("Skipping AWS/ECR preflight for run (--skip-aws).")
    else:
        ext = _extract_ecr_host_region(image_ref)
        if ext:
            host, region = ext
            aws_sts_identity(_default_region(region))
            rc = ecr_login(host, region)
            if rc != 0:
                return rc

    need_pull = bool(force_pull)
    if not need_pull:
        probe = subprocess.run(
            ["docker", "image", "inspect", image_ref],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        need_pull = (probe.returncode != 0)

    if need_pull:
        console.info(f"Pulling image: {image_ref}")
        rc = console.run(["docker", "pull", image_ref])
        if rc != 0:
            machine = _platform.machine().lower()
            is_arm_host = machine in ("arm64", "aarch64")
            if is_arm_host:
                console.warn("Initial pull failed on ARM host. Retrying with '--platform linux/amd64'…")
                rc2 = console.run(["docker", "pull", "--platform", "linux/amd64", image_ref])
                if rc2 == 0:
                    console.success("Pulled amd64 variant successfully.")
                    return 0
                return rc2
            return rc

    return 0
