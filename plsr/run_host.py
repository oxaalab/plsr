from __future__ import annotations

import os
import re
import sys
import shutil
import shlex
import socket
import subprocess
from pathlib import Path
from typing import Optional, List, Tuple

from .console import console

try:
    import tomllib as _toml
except Exception:
    try:
        import tomli as _toml
    except Exception:
        _toml = None


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

def _detect_flavor(text: str) -> str:
    svc = _extract_block(text, "service")
    fl = (_kv_from_block(svc, "flavor") or "").lower().strip()
    if fl in ("mariadb", "mysql", "db-mariadb", "db_mysql"):
        return "db-mariadb"
    return "python-app"

def _env_block(text: str, env_name: str) -> str:
    envs = _extract_block(text, "environments")
    if not envs:
        return ""
    m = re.search(rf"(?ms)^\s*{re.escape(env_name)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", envs)
    return m.group("blk") if m else ""

def _env_port(text: str, env_name: str) -> Optional[int]:
    blk = _env_block(text, env_name)
    p = _kv_from_block(blk, "port") if blk else None
    if not p:
        return None
    try:
        return int(str(p).strip())
    except Exception:
        return None

def _default_app_port(text: str) -> int:
    default = _extract_block(text, "default")
    p = _kv_from_block(default, "APP_PORT")
    try:
        return int(str(p).strip()) if p else 8000
    except Exception:
        return 8000

def _sanitize_pkg(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", name).strip("_") or "app"

def _exists(p: Path) -> bool:
    try:
        return p.is_file()
    except Exception:
        return False


def _pyproject_run_command(root: Path, port: int, env_name: str) -> Optional[List[str]]:
    """
    If pyproject.toml contains:
      [tool.plsr.run]
      command = ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "{port}", "--reload"]
    then return that command with placeholders substituted.
    """
    pp = root / "pyproject.toml"
    if _toml is None or not pp.is_file():
        return None
    try:
        data = _toml.loads(pp.read_text(encoding="utf-8"))
    except Exception:
        return None

    tool = data.get("tool") or {}
    plsr = tool.get("plsr") or {}
    run = plsr.get("run") or {}
    cmd = run.get("command") or run.get("cmd")

    if not cmd:
        return None

    if isinstance(cmd, str):
        tokens = shlex.split(cmd)
    elif isinstance(cmd, list):
        tokens = [str(x) for x in cmd]
    else:
        return None

    mapping = {
        "port": str(port),
        "env": env_name,
        "ENV": env_name,
        "app_env": env_name,
        "APP_ENV": env_name,
    }
    out: List[str] = []
    for t in tokens:
        try:
            out.append(t.format(**mapping))
        except Exception:
            out.append(t)
    return out


def _choose_host_command(
    *,
    root: Path,
    cfg_text: str,
    env_name: str,
    service_name: str,
) -> Tuple[List[str], int]:
    """
    Build the command to run a Python app on host.

    Priority:
      0) pyproject-directed command: [tool.plsr.run].command
      1) uvicorn heuristics (via 'python -m uvicorn' so we don't depend on PATH)
      2) python -m <package> (if package layout exists)
      3) python <main.py/app.py>
      4) final fallback: python -m <sanitized_name>.
    """
    pkg = _sanitize_pkg(service_name)
    port = _env_port(cfg_text, env_name) or _default_app_port(cfg_text)

    cmd_from_pp = _pyproject_run_command(root, port, env_name)
    if cmd_from_pp:
        return (cmd_from_pp, port)

    main_py = root / "main.py"
    app_py = root / "app.py"
    src_main = root / "src" / "main.py"
    src_app = root / "src" / "app.py"
    src_pkg_main = root / "src" / pkg / "main.py"
    src_pkg_app = root / "src" / pkg / "app.py"
    pkg_main = root / pkg / "main.py"
    pkg_app = root / pkg / "app.py"
    pkg_dunder_main = root / pkg / "__main__.py"
    src_pkg_dunder_main = root / "src" / pkg / "__main__.py"


    if _exists(main_py):
        return ([sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)
    if _exists(app_py):
        return ([sys.executable, "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)
    if _exists(src_main):
        return ([sys.executable, "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)
    if _exists(src_app):
        return ([sys.executable, "-m", "uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)
    if _exists(src_pkg_app):
        return ([sys.executable, "-m", "uvicorn", f"{pkg}.app:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)
    if _exists(pkg_app):
        return ([sys.executable, "-m", "uvicorn", f"{pkg}.app:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)
    if _exists(src_pkg_main):
        return ([sys.executable, "-m", "uvicorn", f"{pkg}.main:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)
    if _exists(pkg_main):
        return ([sys.executable, "-m", "uvicorn", f"{pkg}.main:app", "--host", "0.0.0.0", "--port", str(port), "--reload"], port)

    if _exists(pkg_dunder_main) or _exists(src_pkg_dunder_main):
        return ([sys.executable, "-m", pkg], port)
    if _exists(pkg_main) or _exists(src_pkg_main):
        return ([sys.executable, "-m", f"{pkg}.main"], port)
    if _exists(pkg_app) or _exists(src_pkg_app):
        return ([sys.executable, "-m", f"{pkg}.app"], port)

    if _exists(main_py):
        return ([sys.executable, "main.py"], port)
    if _exists(app_py):
        return ([sys.executable, "app.py"], port)

    return ([sys.executable, "-m", pkg], port)


def _is_port_free(port: int, host: str = "0.0.0.0") -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
        return True
    except OSError:
        return False

def _kill_pids_posix(pids: list[int]) -> None:
    import signal
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass

def _free_port(port: int) -> int:
    """
    Try to free a port by killing listeners (local.sh parity).
    POSIX: lsof; Windows: netstat + taskkill.
    """
    if _is_port_free(port):
        return 0

    if os.name != "nt":
        if shutil.which("lsof") is None:
            console.warn(f"lsof not found; cannot auto-free port {port}.")
            return 1
        try:
            res = subprocess.run(["lsof", "-t", f"-i:{port}"], capture_output=True, text=True)
            pids = [int(x) for x in (res.stdout or "").split() if x.strip().isdigit()]
            if not pids:
                return 1
            console.info(f"Attempting to kill listeners on port {port}: {pids}")
            _kill_pids_posix(pids)
        except Exception:
            return 1
    else:
        try:
            res = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, encoding="utf-8", errors="ignore")
            pids: list[int] = []
            for line in (res.stdout or "").splitlines():
                if f":{port} " in line and "LISTENING" in line.upper():
                    parts = line.split()
                    if parts:
                        try:
                            pids.append(int(parts[-1]))
                        except Exception:
                            pass
            if not pids:
                return 1
            console.info(f"Attempting to kill listeners on port {port}: {pids}")
            for pid in pids:
                subprocess.run(["taskkill", "/F", "/PID", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            return 1

    return 0 if _is_port_free(port) else 1


def _remove_pycache_dirs(root: Path) -> None:
    keep = os.getenv("plsr_CLEANUP_PYCACHE", "1").strip().lower() in ("1", "true", "yes", "y")
    if not keep:
        console.info("Skipping __pycache__ cleanup (plsr_CLEANUP_PYCACHE=0).")
        return
    console.info("Removing __pycache__ directories…")
    try:
        for p in root.rglob("__pycache__"):
            try:
                shutil.rmtree(p, ignore_errors=True)
            except Exception:
                pass
    except Exception:
        pass

def _remove_repo_venv(root: Path) -> None:
    keep = os.getenv("plsr_CLEANUP_REPO_VENV", "1").strip().lower() in ("1", "true", "yes", "y")
    if not keep:
        console.info("Keeping repo venv (plsr_CLEANUP_REPO_VENV=0).")
        return
    for name in (".venv", "venv"):
        v = root / name
        if v.is_dir():
            console.info(f"Removing repo venv: {v}")
            try:
                shutil.rmtree(v, ignore_errors=True)
                console.success("Repo venv removed.")
            except Exception as e:
                console.warn(f"Failed to remove {v}: {e}")


def auto_run(
    *,
    env_name: str,
    dry_run: bool = False,
) -> int:
    """
    Run the microservice directly on the host (no Docker).
    For DB services, this is intentionally disabled.
    Always performs cleanup (pycache + .venv) on exit for Python apps.
    """
    root = _app_root()
    cfg = _read_config_text(root)
    name = _kv_top(cfg, "name") or root.name
    flavor = _detect_flavor(cfg or "")

    console.section("Host Runtime")
    console.info(f"Env:    {env_name}")
    console.info(f"Name:   {name}")
    console.info(f"Flavor: {flavor}")
    console.info(f"Root:   {root}")

    if flavor == "db-mariadb":
        console.error("Host (non-Docker) runtime is not supported for database services.")
        console.tip("Run the container instead:  plsr docker run local")
        return 2

    cmd, port = _choose_host_command(root=root, cfg_text=cfg or "", env_name=env_name, service_name=name)

    env = os.environ.copy()
    env.setdefault("ENV", env_name)
    env.setdefault("APP_ENV", env_name)
    env.setdefault("PORT", str(port))
    env.setdefault("APP_PORT", str(port))

    if os.getenv("plsr_FREE_PORT", "1").strip().lower() in ("1", "true", "yes", "y"):
        console.info(f"Ensuring port {port} is free…")
        rc_free = _free_port(port)
        if rc_free != 0:
            console.error(f"Port {port} is in use and could not be freed.")
            console.tip(f"Change environments.{env_name}.port in config.yaml or set plsr_FREE_PORT=0.")
            return 1

    console.info(f"Port:   {port}")
    console.info("Command (host):")
    console.command(cmd, cwd=str(root))

    if dry_run:
        console.info("Dry-run: not executing.")
        return 0

    try:
        return console.run(cmd, cwd=str(root), env=env)
    finally:
        console.section("Cleanup")
        _remove_pycache_dirs(root)
        _remove_repo_venv(root)