from __future__ import annotations

import os
import re
import json
import shutil
import socket
import subprocess
from pathlib import Path
from typing import Optional, Tuple, List

from .console import console
from .aws import ensure_image_pullable
from .build import ensure_db_image_in_ecr

def _app_root() -> Path:
    env = os.getenv("APP_ROOT")
    if env:
        p = Path(env).resolve()
        if p.is_dir():
            return p
    cur = Path.cwd().resolve()
    for p in (cur, *cur.parents):
        if (p / "config.yaml").is_file():
            return p
    return cur

def _read_config_text(root: Path) -> str:
    p = root / "config.yaml"
    if not p.is_file():
        return ""
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def _extract_block(text: str, section: str) -> str:
    m = re.search(rf"(?ms)^\s*{re.escape(section)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", text)
    return m.group("blk") if m else ""

def _kv_from_block(block: str, key: str) -> Optional[str]:
    m = re.search(rf"(?mi)^\s*{re.escape(key)}\s*:\s*['\"]?([^#\r\n'\"]+)['\"]?\s*(?:#.*)?$", block)
    return m.group(1).strip() if m else None

def _kv_top(text: str, key: str) -> Optional[str]:
    m = re.search(rf"(?mi)^\s*{re.escape(key)}\s*:\s*['\"]?([^#\r\n'\"]+)['\"]?\s*(?:#.*)?$", text)
    return m.group(1).strip() if m else None

def _extract_env_block(text: str, env_name: str) -> str:
    envs = _extract_block(text, "environments")
    if not envs:
        return ""
    m = re.search(rf"(?ms)^\s*{re.escape(env_name)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", envs)
    return m.group("blk") if m else ""

def _detect_flavor(text: str) -> str:
    svc = _extract_block(text, "service")
    fl = _kv_from_block(svc, "flavor") or ""
    fl = fl.lower().strip()
    if fl in ("mariadb", "mysql", "db-mariadb", "db_mysql"):
        return "db-mariadb"
    return "python-app"

_PRIV_ECR_HOST = re.compile(r"([0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com)")
_PUB_ECR_HOST = re.compile(r"(public\.ecr\.aws/[A-Za-z0-9-]+)")

def _discover_ecr_root(text: str, env_name: str) -> Optional[str]:
    """
    ECR resolution precedence:
      1) environments.<env>.ECR
      2) top-level ECR
      3) any ECR-like host discovered in references
    """
    env_blk = _extract_env_block(text, env_name)
    ecr_env = _kv_from_block(env_blk, "ECR")
    if ecr_env:
        return ecr_env.strip().rstrip("/")

    ecr_top = _kv_top(text, "ECR")
    if ecr_top:
        return ecr_top.strip().rstrip("/")

    m_priv = _PRIV_ECR_HOST.search(text)
    if m_priv:
        return m_priv.group(1).strip().rstrip("/")
    m_pub = _PUB_ECR_HOST.search(text)
    if m_pub:
        return m_pub.group(1).strip().rstrip("/")
    return None

def _compose_image(text: str, flavor: str, name: str, version: str, env_name: str) -> Optional[str]:
    ecr = _discover_ecr_root(text, env_name)
    if ecr:
        return f"{ecr.rstrip('/')}/{name}:{version}"
    img_block = _extract_block(text, "image")
    local_fb = _kv_from_block(img_block, "local_fallback")
    if local_fb:
        return local_fb
    return f"{name}:{version}" if (name and version) else None

def _env_port(text: str, env_name: str) -> Optional[int]:
    """
    Returns the integer value of environments.<env>.port, if present.
    """
    blk = _extract_env_block(text, env_name)
    if not blk:
        return None
    p = _kv_from_block(blk, "port")
    if not p:
        return None
    try:
        return int(str(p).strip())
    except Exception:
        return None

def _guess_ports(text: str, flavor: str, env_name: str) -> Tuple[int, int]:
    """
    Return (container_port, host_port).

    DB rule (MariaDB/MySQL):
      - Container port is ALWAYS 3306 (server listens there).
      - Host port can be overridden via environments.<env>.port; otherwise falls
        back to service.port (if numeric) or 3306.

    App rule:
      - Container from default.APP_PORT (default 8000),
      - Host from environments.<env>.port (fallback to container).
    """
    if flavor == "db-mariadb":
        container_port = 3306
        env_p = _env_port(text, env_name)
        if env_p:
            return container_port, env_p
        svc = _extract_block(text, "service")
        p = _kv_from_block(svc, "port")
        host_port = int(p) if p and str(p).isdigit() else 3306
        return container_port, host_port

    default = _extract_block(text, "default")
    p = _kv_from_block(default, "APP_PORT")
    cport = int(p) if p and p.isdigit() else 8000
    hport = _env_port(text, env_name) or cport
    return cport, hport

def _data_dir_container(text: str) -> str:
    svc = _extract_block(text, "service")
    d = _kv_from_block(svc, "data_dir")
    return d or "/var/lib/mysql"

def _dev_root_password(text: str) -> Optional[str]:
    """
    Legacy fallback: read dev.root.password from top-level dev block.
    """
    dev = _extract_block(text, "dev")
    if not dev:
        dev = _extract_block(text, "dev")
    m = re.search(r"(?ms)^\s*root\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", dev)
    blk = m.group("blk") if m else dev
    return _kv_from_block(blk, "password")

def _env_root_password(text: str, env_name: str) -> Optional[str]:
    """
    Preferred: read environments.<env>.root_pw when present.
    """
    blk = _extract_env_block(text, env_name)
    if not blk:
        return None
    val = _kv_from_block(blk, "root_pw")
    return val.strip() if val else None

def _sanitize_name(n: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", n)[:63] or "plsr-service"

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _host_data_dir_from_config(text: str, env_name: str) -> Optional[Path]:
    blk = _extract_env_block(text, env_name)
    if not blk:
        return None
    raw = _kv_from_block(blk, "data_dir")
    if not raw:
        return None
    return Path(raw.strip()).expanduser().resolve()

def _default_local_db_dir() -> Path:
    return Path.home() / "dev" / "plsr" / "local" / "db"


def _is_port_free(port: int, host: str = "0.0.0.0") -> bool:
    """
    Try to bind to host:port to determine if it's free.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
        return True
    except OSError:
        return False

def _find_free_port(start: int, attempts: int = 50) -> Optional[int]:
    """
    Find the first free port in [start, start+attempts).
    """
    for p in range(start, start + max(1, attempts)):
        if _is_port_free(p):
            return p
    return None


def _docker_inspect_json(name: str) -> Optional[dict]:
    res = subprocess.run(["docker", "inspect", name], capture_output=True, text=True)
    if res.returncode != 0:
        return None
    try:
        arr = json.loads(res.stdout or "[]")
        return arr[0] if isinstance(arr, list) and arr else None
    except Exception:
        return None

def _db_mount_host_dir_from_inspect(obj: dict, container_data_path: str) -> Optional[Path]:
    mounts = obj.get("Mounts", []) if isinstance(obj, dict) else []
    for m in mounts:
        if m.get("Destination") == container_data_path and m.get("Type") == "bind":
            src = m.get("Source")
            if src:
                return Path(src).expanduser().resolve()
    return None

def _allowed_delete_host_dir(host_dir: Path, root: Path, cfg_text: str, env_name: str) -> bool:
    bases = [
        _default_local_db_dir().resolve(),
        (root / ".plsr" / "data" / "mariadb").resolve(),
    ]
    cfg_path = _host_data_dir_from_config(cfg_text, env_name)
    if cfg_path:
        bases.append(cfg_path.resolve())
    host_dir = host_dir.resolve()
    for base in bases:
        base = base.resolve()
        if host_dir == base or host_dir.is_relative_to(base):
            return True
    home_plsr = (Path.home() / "dev" / "plsr").resolve()
    if host_dir.is_relative_to(home_plsr):
        return True
    return False


def _db_container_name(cfg_text: str, service_name: str) -> str:
    """
    For DB services, prefer dev.compose.container_name; otherwise use top-level name.
    No environment suffix is appended.
    """
    dev_blk = _extract_block(cfg_text, "dev")
    comp_blk = None
    if dev_blk:
        m = re.search(r"(?ms)^\s*compose\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", dev_blk)
        comp_blk = m.group("blk") if m else None
    cname = _kv_from_block(comp_blk or "", "container_name") if comp_blk else None
    return _sanitize_name(cname or service_name)


def _confirm(prompt: str, default_no: bool = True) -> bool:
    """
    Ask a yes/no question. Returns True for yes.
    Env bypass:
      PLSR_AUTO_REDEPLOY=1 → assume yes
      PLSR_AUTO_REDEPLOY=0 → assume no
    """
    env = os.getenv("PLSR_AUTO_REDEPLOY")
    if env is not None:
        return env.strip().lower() in ("1", "true", "yes", "y")

    suffix = " [y/N] " if default_no else " [Y/n] "
    try:
        ans = input(prompt + suffix).strip().lower()
    except EOFError:
        ans = ""
    if not ans:
        return not default_no
    return ans in ("y", "yes")


def auto_run(
    *,
    env_name: str,
    image_override: Optional[str] = None,
    name_override: Optional[str] = None,
    port_overrides: List[str] | None = None,
    env_overrides: List[str] | None = None,
    data_host_dir: Optional[str] = None,
    detach: bool = True,
    force_pull: bool = False,
    skip_aws: bool = False,
    dry_run: bool = False,
) -> int:
    if not env_name:
        console.error("Environment name is required.")
        console.tip("Use: plsr docker run <env> [options]")
        console.info("Examples:")
        console.info("  ./ctl.sh docker run local")
        return 2

    if shutil.which("docker") is None:
        console.error("docker not found on PATH.")
        return 1

    root = _app_root()
    cfg = _read_config_text(root)
    if not cfg:
        console.warn(f"No config.yaml found under {root}. Using conservative defaults.")

    name = _kv_top(cfg, "name") or root.name
    version = _kv_top(cfg, "version") or "0.0.0"
    flavor = _detect_flavor(cfg or "")

    if flavor == "db-mariadb":
        cname = _db_container_name(cfg, name)
        legacy_name = _sanitize_name(f"{name}-{env_name}")
        if legacy_name != cname:
            insp_legacy = _docker_inspect_json(legacy_name)
            if insp_legacy is not None:
                state = (insp_legacy.get("State") or {})
                running = bool(state.get("Running"))
                status = state.get("Status") or ("running" if running else "exited")
                console.warn(f"Legacy container '{legacy_name}' detected (state: {status}). Removing…")
                rm_cmd = ["docker", "rm"]
                if running:
                    rm_cmd.append("-f")
                rm_cmd.append(legacy_name)
                rm_rc = console.run(rm_cmd, cwd=str(root))
                if rm_rc != 0:
                    return rm_rc
    else:
        cname_default = f"{name}-{env_name}"
        cname = _sanitize_name(name_override or cname_default)

    insp_existing = _docker_inspect_json(cname)
    wants_redeploy = False

    if insp_existing is not None:
        state = (insp_existing.get("State") or {})
        is_running = bool(state.get("Running"))
        status = state.get("Status") or ("running" if is_running else "exited")

        if is_running:
            console.info(f"Container '{cname}' is currently {status}.")
            if not _confirm("Redeploy the container now?", default_no=True):
                console.info("OK. Leaving the running container as-is.")
                return 0
            wants_redeploy = True
        else:
            console.warn(f"Container '{cname}' exists but is not running. It will be replaced.")
            wants_redeploy = True

    if flavor == "db-mariadb":
        _, rc_ensure = ensure_db_image_in_ecr(mode="run")
        if rc_ensure != 0:
            return rc_ensure
    elif wants_redeploy and flavor == "db-mariadb":
        _, rc = ensure_db_image_in_ecr(mode="run")
        if rc != 0:
            return rc

    image = image_override or _compose_image(cfg, flavor, name, version, env_name)
    if not image:
        console.error("Unable to determine docker image (set --image or add ECR/name/version in config.yaml).")
        return 2

    container_port, host_port = _guess_ports(cfg, flavor, env_name)

    if not port_overrides:
        strict = os.getenv("PLSR_STRICT_PORT", "0").strip().lower() in ("1", "true", "yes", "y")
        if not _is_port_free(host_port):
            if strict:
                console.error(f"Host port {host_port} is already in use. Refusing to auto-shift due to PLSR_STRICT_PORT=1.")
                console.tip(f"macOS:   lsof -nP -iTCP:{host_port} | grep LISTEN")
                console.tip(f"Docker:  docker ps --format '{{{{.Names}}}} -> {{{{.Ports}}}}' | grep :{host_port}")
                console.tip(f"Change environments.{env_name}.port in config.yaml, stop the blocker, or run with -p <host>:<cont>.")
                return 1
            new_port = _find_free_port(host_port + 1, attempts=50)
            if new_port is None:
                console.error(f"No free port found near {host_port}. Try specifying -p <host>:<cont> or set environments.{env_name}.port.")
                return 1
            console.warn(f"Host port {host_port} is in use. Using next available port: {new_port}")
            console.tip(f"Connect to the DB on host {new_port} (container {container_port}).")
            host_port = new_port

    if wants_redeploy:
        insp_now = _docker_inspect_json(cname)
        if insp_now is not None:
            now_running = bool((insp_now.get("State") or {}).get("Running"))
            rm_cmd = ["docker", "rm"]
            if now_running:
                rm_cmd.append("-f")
            rm_cmd.append(cname)
            rm_rc = console.run(rm_cmd, cwd=str(root))
            if rm_rc != 0:
                return rm_rc

        if flavor == "db-mariadb":
            data_cont = _data_dir_container(cfg)
            host_dir = _db_mount_host_dir_from_inspect(insp_existing or {}, data_cont)
            if host_dir is None:
                host_dir = _host_data_dir_from_config(cfg, env_name) or _default_local_db_dir()
            host_dir = host_dir.expanduser().resolve()

            if _allowed_delete_host_dir(host_dir, root, cfg, env_name):
                if host_dir.exists():
                    try:
                        shutil.rmtree(host_dir)
                        console.success(f"Deleted DB data dir: {host_dir}")
                    except Exception as e:
                        console.warn(f"Failed to delete DB data dir {host_dir}: {e}")
                _ensure_dir(host_dir)
                console.success(f"Recreated DB data dir: {host_dir}")
            else:
                console.warn(f"Refusing to delete non-whitelisted path: {host_dir}")
                console.tip("Adjust environments.<env>.data_dir to a safe path under ~/dev/plsr or use the default.")

    force_pull_effective = bool(force_pull or (wants_redeploy and flavor == "db-mariadb"))
    rc = ensure_image_pullable(image, allow_skip=bool(skip_aws), force_pull=force_pull_effective)
    if rc != 0:
        return rc

    cmd: List[str] = ["docker", "run"]
    if detach:
        cmd.append("-d")
    cmd += ["--name", cname]

    if port_overrides:
        for mapping in port_overrides:
            mp = (mapping or "").strip()
            if not mp:
                continue
            if ":" in mp:
                cmd += ["-p", mp]
            else:
                cmd += ["-p", f"{mp}:{mp}"]
    else:
        cmd += ["-p", f"{host_port}:{container_port}"]

    have_app_env = any(str(s).startswith("APP_ENV=") for s in (env_overrides or []))
    for e in (env_overrides or []):
        if "=" in e:
            cmd += ["-e", e]
    if not have_app_env:
        cmd += ["-e", f"APP_ENV={env_name}"]

    if flavor == "db-mariadb":
        data_cont = _data_dir_container(cfg)
        if data_host_dir:
            host_dir_for_run = Path(data_host_dir).expanduser().resolve()
        else:
            host_dir_for_run = _host_data_dir_from_config(cfg, env_name) or _default_local_db_dir()
        _ensure_dir(host_dir_for_run)
        cmd += ["-v", f"{str(host_dir_for_run)}:{data_cont}"]

        have_root_pw_override = any(
            str(s).startswith(("MARIADB_ROOT_PASSWORD=", "MYSQL_ROOT_PASSWORD="))
            for s in (env_overrides or [])
        )
        if not have_root_pw_override:
            pw = _env_root_password(cfg, env_name) or _dev_root_password(cfg) or "Password1"
            cmd += ["-e", f"MARIADB_ROOT_PASSWORD={pw}"]
            cmd += ["-e", f"MYSQL_ROOT_PASSWORD={pw}"]

    cmd.append(image)

    console.section("Local Runner")
    console.info(f"Env:    {env_name}")
    console.info(f"Flavor: {flavor}")
    console.info(f"Image:  {image}")
    console.info(f"Name:   {cname}")
    console.info(f"Ports:  host {host_port} -> container {container_port}")

    if dry_run:
        console.info("Dry run (not executing):")
        console.command(cmd)
        return 0

    return console.run(cmd, cwd=str(root))


def auto_stop(
    *,
    env_name: str,
    name_override: Optional[str] = None,
    keep_data: bool = False,
    dry_run: bool = False,
) -> int:
    if not env_name:
        console.error("Environment name is required.")
        console.tip("Use: plsr docker stop <env> [options]")
        console.info("Examples:")
        console.info("  ./ctl.sh docker stop local")
        return 2

    if shutil.which("docker") is None:
        console.error("docker not found on PATH.")
        return 1

    root = _app_root()
    cfg = _read_config_text(root)

    name = _kv_top(cfg, "name") or root.name
    flavor = _detect_flavor(cfg or "")
    data_cont = _data_dir_container(cfg) if flavor == "db-mariadb" else None

    if flavor == "db-mariadb":
        cname = _db_container_name(cfg, name)
    else:
        cname_default = f"{name}-{env_name}"
        cname = _sanitize_name(name_override or cname_default)

    console.section("Local Stop")
    console.info(f"Env:    {env_name}")
    console.info(f"Name:   {cname}")
    console.info(f"Flavor: {flavor}")

    insp = _docker_inspect_json(cname)
    if insp is None:
        console.warn(f"Container '{cname}' not found.")
    else:
        state = (insp.get("State") or {}).get("Status") or "unknown"
        console.info(f"Current state: {state}")

    actions: List[List[str]] = []
    if insp is not None and (insp.get("State") or {}).get("Running"):
        actions.append(["docker", "stop", cname])
    if insp is not None:
        actions.append(["docker", "rm", cname])

    host_dir: Optional[Path] = None
    if flavor == "db-mariadb" and not keep_data:
        host_dir = _db_mount_host_dir_from_inspect(insp or {}, data_cont or "/var/lib/mysql") if insp else None
        if host_dir is None:
            host_dir = _host_data_dir_from_config(cfg, env_name) or _default_local_db_dir()

    if dry_run:
        console.info("Dry run (not executing). Planned actions:")
        for a in actions:
            console.command(a)
        if host_dir:
            console.info(f"Would delete data dir: {host_dir}")
        return 0

    rc = 0
    for a in actions:
        step = console.run(a, cwd=str(root))
        rc = rc or step

    if host_dir:
        host_dir = host_dir.expanduser().resolve()
        if host_dir.exists():
            if _allowed_delete_host_dir(host_dir, root, cfg, env_name):
                try:
                    shutil.rmtree(host_dir)
                    console.success(f"Deleted DB data dir: {host_dir}")
                except Exception as e:
                    console.error(f"Failed to delete {host_dir}: {e}")
                    rc = rc or 1
            else:
                console.warn(f"Refusing to delete non-whitelisted path: {host_dir}")
                console.tip("Use --keep-data to skip deletion or adjust environments.<env>.data_dir to a safe path.")
        else:
            console.info(f"Data dir not found (nothing to delete): {host_dir}")

    return rc
