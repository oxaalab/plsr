from __future__ import annotations

import os
import re
import sys
import shutil
import subprocess
import tempfile
from pathlib import Path

from .pyvenv import should_use_repo_venv, spawn_in_repo_venv


def _detect_app_root() -> Path:
    """
    Determine the microservice root directory.
    Priority:
      1) APP_ROOT env var (only if it contains config.yaml or pyproject.toml)
      2) Walk up from CWD looking for config.yaml or pyproject.toml
      3) CWD fallback
    """
    env_root = os.getenv("APP_ROOT")
    if env_root:
        p = Path(env_root).resolve()
        if p.is_dir() and ((p / "config.yaml").is_file() or (p / "pyproject.toml").is_file()):
            return p

    cur = Path.cwd().resolve()
    for p in (cur, *cur.parents):
        if (p / "config.yaml").is_file() or (p / "pyproject.toml").is_file():
            return p
    return cur


def _read_name_version_from_config_yaml(app_root: Path) -> tuple[str | None, str | None]:
    """
    Read only top-level 'name' and 'version' from config.yaml without external deps.
    Quoted or unquoted values are supported.
    """
    cfg = app_root / "config.yaml"
    if not cfg.is_file():
        return None, None
    try:
        text = cfg.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None, None

    m_name = re.search(r'(?m)^\s*name\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
    m_ver = re.search(r'(?m)^\s*version\s*:\s*["\']?([^"\']+?)["\']?\s*$', text)
    name = m_name.group(1).strip() if m_name else None
    ver = m_ver.group(1).strip() if m_ver else None
    return name, ver


def _read_name_version_from_pyproject(app_root: Path) -> tuple[str | None, str | None]:
    """
    Best-effort fallback: read name/version from pyproject.toml (PEP 621 or Poetry).
    """
    try:
        import tomllib as _toml
    except Exception:
        _toml = None
        try:
            import tomli as _toml
        except Exception:
            _toml = None

    pp = app_root / "pyproject.toml"
    if not pp.is_file() or _toml is None:
        return None, None
    try:
        data = _toml.loads(pp.read_text(encoding="utf-8"))
    except Exception:
        return None, None

    name = ""
    version = ""

    proj = data.get("project") or {}
    name = proj.get("name", "") or name
    version = proj.get("version", "") or version

    tool = data.get("tool") or {}
    poetry = tool.get("poetry") or {}
    name = name or poetry.get("name", "") or name
    version = version or poetry.get("version", "") or version

    return (name or None), (version or None)


def _export_identity_env(app_name: str, app_version: str) -> None:
    """
    Export identity variables for downstream tools.
    We avoid overwriting existing values (allowing external overrides).
    """
    if app_name:
        os.environ.setdefault("SERVICE_NAME", app_name)
        os.environ.setdefault("APP_NAME", app_name)
    if app_version:
        os.environ.setdefault("SERVICE_VERSION", app_version)
        os.environ.setdefault("APP_VERSION", app_version)


def _in_venv() -> bool:
    if os.getenv("PULSAR_VENV_ACTIVE") == "1":
        return True
    base = getattr(sys, "base_prefix", None) or getattr(sys, "prefix", None)
    return bool(hasattr(sys, "base_prefix") and sys.prefix != sys.base_prefix)


def _venv_python_path(venv_dir: Path) -> Path:
    if os.name == "nt":
        cand = venv_dir / "Scripts" / "python.exe"
        return cand
    for name in ("python3", "python"):
        cand = venv_dir / "bin" / name
        if cand.exists():
            return cand
    return venv_dir / "bin" / "python"


def _run_cli_inline(argv: list[str]) -> int:
    """
    Run the Pulsar CLI in the current interpreter (assumes already inside venv).
    """
    app_root = _detect_app_root()
    name, ver = _read_name_version_from_config_yaml(app_root)
    if not name or not ver:
        name2, ver2 = _read_name_version_from_pyproject(app_root)
        name = name or name2 or app_root.name
        ver = ver or ver2 or "0.0.0"
    _export_identity_env(name, ver)

    from pulsar.cli import run_from_args
    return run_from_args(argv)


def _spawn_in_temp_venv_and_cleanup(argv: list[str]) -> int:
    """
    Create a temporary virtual environment, re-invoke this module inside it,
    wait for completion, then delete the venv directory (cleanup).
    """
    pkg_root = Path(__file__).resolve().parent.parent.parent
    tmp_dir = Path(tempfile.mkdtemp(prefix="pulsar-venv-"))

    try:
        subprocess.run([sys.executable, "-m", "venv", str(tmp_dir)], check=True)

        child_env = os.environ.copy()
        child_env["PULSAR_VENV_ACTIVE"] = "1"

        existing_pp = child_env.get("PYTHONPATH")
        child_env["PYTHONPATH"] = (
            f"{pkg_root}{os.pathsep}{existing_pp}" if existing_pp else str(pkg_root)
        )

        vpy = _venv_python_path(tmp_dir)
        cmd = [str(vpy), "-m", "pulsar.bootstrap", *argv]
        result = subprocess.run(cmd, env=child_env)
        code = int(result.returncode)

        return code
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except Exception as e:
            print(f"[pulsar] Warning: failed to remove temp venv {tmp_dir}: {e}", file=sys.stderr)


def main() -> None:
    """
    Entry point that guarantees Pulsar runs inside a virtual env.

    For 'start local' on Python microservices, we prefer a repo‑local venv
    (e.g., <APP_ROOT>/.venv) — see pulsar.core.pyvenv — keeping app deps local.
    Other commands keep the ephemeral-venv behavior.
    """
    argv = sys.argv[1:]

    if _in_venv():
        exit_code = _run_cli_inline(argv)
        raise SystemExit(exit_code)

    try:
        if should_use_repo_venv(argv):
            code = spawn_in_repo_venv(argv)
            raise SystemExit(code)
    except Exception:
        pass

    exit_code = _spawn_in_temp_venv_and_cleanup(argv)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
