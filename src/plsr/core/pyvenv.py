from __future__ import annotations

import os
import re
import sys
import shutil
import subprocess
import hashlib
import signal
from pathlib import Path
from typing import List

from .console import console

try:
    import tomllib as _toml
except Exception:
    try:
        import tomli as _toml
    except Exception:
        _toml = None


def _is_app_root_dir(p: Path) -> bool:
    """
    A directory 'looks like' a microservice root if it contains either:
      • config.yaml, or
      • pyproject.toml
    """
    try:
        return ((p / "config.yaml").is_file()) or ((p / "pyproject.toml").is_file())
    except Exception:
        return False


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
        p = Path(env_root).expanduser().resolve()
        if p.is_dir() and _is_app_root_dir(p):
            return p

    cur = Path.cwd().resolve()
    for p in (cur, *cur.parents):
        if _is_app_root_dir(p):
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


def _extract_block(text: str, section: str) -> str:
    m = re.search(rf"(?ms)^\s*{re.escape(section)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", text)
    return m.group("blk") if m else ""


def _kv_from_block(block: str, key: str) -> str | None:
    m = re.search(rf"(?mi)^\s*{re.escape(key)}\s*:\s*['\"]?([^#\r\n'\"]+)['\"]?\s*(?:#.*)?$", block)
    return m.group(1).strip() if m else None


def _detect_flavor(app_root: Path) -> str:
    """
    Guess microservice flavor from config.yaml (or default).
    Returns: "db-mariadb" or "python-app"

    EXPLICIT-ONLY: classify as db-mariadb only when service.flavor is explicitly
    set to mariadb/mysql.
    """
    text = _read_text_config(app_root)
    if not text:
        return "python-app"
    svc = _extract_block(text, "service")
    fl = (_kv_from_block(svc, "flavor") or "").lower().strip()
    if fl in ("mariadb", "mysql", "db-mariadb", "db_mysql"):
        return "db-mariadb"
    return "python-app"


def _venv_python_path(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    for name in ("python3", "python"):
        cand = venv_dir / "bin" / name
        if cand.exists():
            return cand
    return venv_dir / "bin" / "python"


def _read_pyproject(app_root: Path) -> dict:
    if _toml is None:
        return {}
    pp = app_root / "pyproject.toml"
    if not pp.is_file():
        return {}
    try:
        return _toml.loads(pp.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _min_py_from_requires(spec: str | None) -> tuple[int, int] | None:
    """
    Very small parser for '>=3.12,<4.0' / '^3.12' / '==3.12.*' shapes.
    Returns (major, minor) minimum or None.
    """
    if not spec:
        return None
    s = spec.replace(" ", "")
    m = re.search(r">=3\.(\d+)", s)
    if m:
        return 3, int(m.group(1))
    m = re.search(r"\^3\.(\d+)", s)  # ^3.12 → >=3.12,<4
    if m:
        return 3, int(m.group(1))
    m = re.search(r"==3\.(\d+)", s)
    if m:
        return 3, int(m.group(1))
    m = re.search(r"3\.(\d+)", s)
    if m:
        return 3, int(m.group(1))
    return None


def _select_python_for_app(app_root: Path) -> str:
    """
    Try to honor project.requires-python by picking python3.X if available; otherwise
    use the current interpreter and warn if it doesn't meet the minimum.
    """
    data = _read_pyproject(app_root)
    requires = ((data.get("project") or {}).get("requires-python")
                or (((data.get("tool") or {}).get("poetry") or {}).get("dependencies") or {}).get("python"))
    minver = _min_py_from_requires(str(requires) if requires else None)

    if not minver:
        return sys.executable

    req_major, req_minor = minver
    cur_major, cur_minor = sys.version_info[:2]

    if (cur_major, cur_minor) >= (req_major, req_minor):
        return sys.executable

    if os.name != "nt":
        for cand in (f"python{req_major}.{req_minor}", f"python{req_major}"):
            path = shutil.which(cand)
            if not path:
                continue
            try:
                out = subprocess.check_output(
                    [path, "-c", "import sys;print('.'.join(map(str,sys.version_info[:2])))"],
                    text=True
                ).strip()
                parts = out.split(".")
                if len(parts) >= 2 and (int(parts[0]), int(parts[1])) >= (req_major, req_minor):
                    return path
            except Exception:
                continue

    console.warn(
        f"Requested Python {req_major}.{req_minor}+ per pyproject.toml, "
        f"but using current interpreter {cur_major}.{cur_minor}."
    )
    return sys.executable


def _hash_inputs_for_deps(app_root: Path) -> str:
    """
    Hash inputs that drive dependency resolution. If this hash changes, we re-install.
    """
    h = hashlib.sha256()
    for name in ("pyproject.toml", "poetry.lock", "uv.lock", "requirements.txt"):
        p = app_root / name
        try:
            if p.is_file():
                h.update(p.read_bytes())
        except Exception:
            pass
    return h.hexdigest()


def _has_setup_files(app_root: Path) -> bool:
    return (app_root / "setup.py").is_file() or (app_root / "setup.cfg").is_file()


def _has_requirements_txt(app_root: Path) -> bool:
    return (app_root / "requirements.txt").is_file()


def _pyproject_meta(app_root: Path) -> dict:
    return _read_pyproject(app_root) or {}


def _is_pep621_with_name(meta: dict) -> bool:
    proj = meta.get("project") or {}
    return bool(proj.get("name"))


def _is_poetry_project(meta: dict) -> bool:
    tool = meta.get("tool") or {}
    poetry = tool.get("poetry") or {}
    return bool(poetry.get("name"))


def _project_dependencies(meta: dict) -> List[str]:
    """
    Extract [project].dependencies for app-style repos (no package build step).
    """
    proj = meta.get("project") or {}
    deps = proj.get("dependencies") or []
    out: List[str] = []
    for d in deps:
        s = str(d).strip()
        if s:
            out.append(s)
    return out


def _install_deps_from_project_table(app_root: Path, vpy: str, meta: dict) -> int:
    deps = _project_dependencies(meta)
    if not deps:
        return 0
    console.info("Installing dependencies from pyproject [project.dependencies]")
    cmd = [vpy, "-m", "pip", "install", *deps]
    return console.run(cmd, cwd=str(app_root))


def _should_try_editable(app_root: Path, meta: dict) -> tuple[bool, str]:
    """
    Decide if we should attempt 'pip install -e .'.
    Returns (should_try, reason).
    """
    if _has_setup_files(app_root):
        return True, "setup.py/setup.cfg present"
    if _is_pep621_with_name(meta):
        return True, "pyproject [project.name] present"
    if _is_poetry_project(meta):
        return True, "pyproject [tool.poetry.name] present"
    return False, "no package metadata detected (app-only repo)"


def _install_requirements_if_present(app_root: Path, vpy: str) -> int:
    if _has_requirements_txt(app_root):
        console.info("Installing dependencies from requirements.txt")
        return console.run([vpy, "-m", "pip", "install", "-r", "requirements.txt"], cwd=str(app_root))
    console.warn("No requirements.txt found; skipping dependency install.")
    return 0


def _install_project_deps_if_needed(app_root: Path, venv_dir: Path, vpy: str) -> int:
    """
    Core dependency installer:
      • Upgrade pip/setuptools/wheel,
      • If repo is a package (setup.py or [project.name] or [tool.poetry.name]) → editable install,
        and if editable fails for Poetry projects → fallback to non-editable `pip install .`,
      • Else if app-only → install from [project.dependencies],
      • Fallback to requirements.txt when provided.
    """
    marker = venv_dir / ".pulsar-deps.sha256"
    digest = _hash_inputs_for_deps(app_root)

    force = os.getenv("PULSAR_FORCE_DEPS", "").strip().lower() in ("1", "true", "yes", "y")

    need = True
    if marker.exists() and not force:
        try:
            if marker.read_text(encoding="utf-8").strip() == digest:
                need = False
        except Exception:
            need = True

    if not need:
        return 0

    console.section("Python dependencies")
    console.info(f"Installing project deps into venv: {venv_dir}")

    rc = console.run([vpy, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], cwd=str(app_root))
    if rc != 0:
        return rc

    meta = _pyproject_meta(app_root)

    try_editable, reason = _should_try_editable(app_root, meta)
    if try_editable:
        extras = os.getenv("PULSAR_INSTALL_EXTRAS", "").strip()
        install_spec = f".[{extras}]" if extras else "."
        console.info(f"Attempting editable install ({reason})")
        rc_edit = console.run([vpy, "-m", "pip", "install", "-e", install_spec], cwd=str(app_root))
        if rc_edit == 0:
            try:
                marker.write_text(digest, encoding="utf-8")
            except Exception:
                pass
            console.success("Dependencies are installed and up to date.")
            return 0

        if _is_poetry_project(meta):
            console.warn("Editable install failed; attempting standard install for Poetry project.")
            rc_std = console.run([vpy, "-m", "pip", "install", install_spec], cwd=str(app_root))
            if rc_std == 0:
                try:
                    marker.write_text(digest, encoding="utf-8")
                except Exception:
                    pass
                console.success("Dependencies are installed and up to date.")
                return 0

        console.warn("Editable install failed; falling back to app-style/requirements install.")

    rc_app = _install_deps_from_project_table(app_root, vpy, meta)
    if rc_app != 0:
        return rc_app

    rc_req = _install_requirements_if_present(app_root, vpy)
    if rc_req != 0:
        return rc_req

    try:
        marker.write_text(digest, encoding="utf-8")
    except Exception:
        pass
    console.success("Dependencies are installed and up to date.")
    return 0


def _create_or_update_venv(venv_dir: Path, app_root: Path) -> tuple[str, int]:
    """
    Create venv if missing (using a Python that satisfies project.requires-python when possible),
    then ensure project dependencies are installed.
    Returns (venv_python_path, exit_code).
    """
    py_venv_cfg = venv_dir / "pyvenv.cfg"
    if not py_venv_cfg.is_file():
        venv_dir.mkdir(parents=True, exist_ok=True)
        console.section("Creating local Python venv")
        console.info(f"Path:   {venv_dir}")

        interp = _select_python_for_app(app_root)
        rc = console.run([interp, "-m", "venv", str(venv_dir)])
        if rc != 0:
            return "", rc

    vpy = str(_venv_python_path(venv_dir))
    rc_deps = _install_project_deps_if_needed(app_root, venv_dir, vpy)
    return vpy, rc_deps


def _load_dotenv_into(dst_env: dict, app_root: Path) -> None:
    """
    Best-effort loader of .env.<ENV> (or .env) into dst_env without overriding
    variables already set by the parent process. Also prepends '<APP_ROOT>/src'
    to PYTHONPATH to mirror local.sh behavior.
    """
    try:
        env_name = (dst_env.get("PULSAR_ENV") or dst_env.get("ENV") or "local").strip()
        candidates = [
            app_root / f".env.{env_name}",
            app_root / f"env.{env_name}",
            app_root / ".env",
        ]
        keyval = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")
        for path in candidates:
            if not path.is_file():
                continue
            console.info(f"Loading environment from {path}")
            for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                if not raw or raw.lstrip().startswith("#"):
                    continue
                m = keyval.match(raw)
                if not m:
                    continue
                k, v = m.group(1), m.group(2)
                v = v.strip()
                if len(v) >= 2 and ((v[0] == v[-1]) and v[0] in ("'", '"')):
                    v = v[1:-1]
                if k not in dst_env:
                    dst_env[k] = v
            break
    except Exception:
        pass

    try:
        src_dir = app_root / "src"
        if src_dir.is_dir():
            existing_pp = dst_env.get("PYTHONPATH") or ""
            if existing_pp:
                dst_env["PYTHONPATH"] = f"{str(src_dir)}{os.pathsep}{existing_pp}"
            else:
                dst_env["PYTHONPATH"] = str(src_dir)
    except Exception:
        pass


def _extract_env_from_argv(argv: list[str]) -> str | None:
    """
    Support both 'pulsar start <env>' and 'pulsar <env> start' shapes.
    """
    if not argv:
        return None
    if argv[0] == "start" and len(argv) >= 2:
        return argv[1]
    if argv[0] == "app" and len(argv) >= 3 and argv[1] == "start":
        return argv[2]
    if len(argv) >= 2 and argv[1] == "start":
        return argv[0]
    return None


def should_use_repo_venv(argv: list[str]) -> bool:
    """
    Decide whether to run inside a repo‑local venv instead of the ephemeral venv.

    Conditions (all must hold):
      • We are handling a 'start' invocation (either 'start <env>' or '<env> start'),
      • Env is 'local' (exact),
      • The current microservice flavor is *not* 'db-mariadb' (i.e., it's a Python app),
      • Not explicitly disabled via PULSAR_SKIP_REPO_VENV=1.
    Force enable with PULSAR_FORCE_REPO_VENV=1.
    """
    if os.getenv("PULSAR_SKIP_REPO_VENV", "").strip() in ("1", "true", "yes", "y"):
        return False
    if os.getenv("PULSAR_FORCE_REPO_VENV", "").strip() in ("1", "true", "yes", "y"):
        return True

    tokens = [t for t in (argv or []) if t and not t.startswith("-")]
    has_start = ("start" in tokens[:3])
    if not has_start:
        return False

    env_in_argv = _extract_env_from_argv(argv)
    env_in_env = os.getenv("PULSAR_ENV") or os.getenv("ENV")
    env_name = (env_in_argv or env_in_env or "local").strip().lower()
    if env_name != "local":
        return False

    app_root = _detect_app_root()
    flavor = _detect_flavor(app_root)
    return flavor != "db-mariadb"


def _purge_pycache_dirs(app_root: Path) -> None:
    """
    Delete __pycache__ trees and common Python temp directories under app_root.
    """
    console.info("Removing __pycache__ directories…")
    try:
        for root, dirs, _files in os.walk(app_root):
            for d in list(dirs):
                if d == "__pycache__":
                    p = Path(root) / d
                    try:
                        shutil.rmtree(p, ignore_errors=True)
                    except Exception:
                        pass
    except Exception:
        pass

    extras_dirs = [".pytest_cache", ".mypy_cache", ".ruff_cache", ".tox", "build", "dist", "htmlcov"]
    for name in extras_dirs:
        try:
            p = app_root / name
            if p.exists():
                if p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
                else:
                    p.unlink(missing_ok=True)
        except Exception:
            pass
    for f in [".coverage"]:
        try:
            fp = app_root / f
            if fp.is_file():
                fp.unlink()
        except Exception:
            pass


def _cleanup_repo_runtime(app_root: Path, venv_dir: Path) -> None:
    """
    Cleanup routine for Python app runtime:
      • Remove all __pycache__ dirs and common test/build caches under the repo,
      • Remove the repo-local virtualenv directory.
    Can be skipped with PULSAR_KEEP_REPO_VENV=1.
    """
    if os.getenv("PULSAR_KEEP_REPO_VENV", "").strip().lower() in ("1", "true", "yes", "y"):
        console.warn("Skipping cleanup (PULSAR_KEEP_REPO_VENV=1).")
        return

    console.section("Cleanup")
    _purge_pycache_dirs(app_root)

    try:
        if venv_dir.exists():
            console.info(f"Removing repo venv: {venv_dir}")
            shutil.rmtree(venv_dir, ignore_errors=True)
            console.success("Repo venv removed.")
    except Exception:
        pass


def spawn_in_repo_venv(argv: list[str]) -> int:
    """
    Ensure a repo‑local venv exists (preferring <APP_ROOT>/.venv) and re‑invoke
    Pulsar inside it, preserving on PYTHONPATH.
    Also loads .env files into the child environment (best effort).

    On exit (normal or via Ctrl‑C), clean up: remove __pycache__ dirs and the
    repo venv, unless PULSAR_KEEP_REPO_VENV=1 is set.
    """
    app_root = _detect_app_root()

    override = os.getenv("PULSAR_VENV_DIR")
    if override:
        venv_dir = Path(override).expanduser().resolve()
    else:
        venv_dir = (app_root / ".venv")
        alt = (app_root / "venv")
        if not venv_dir.exists() and alt.is_dir():
            venv_dir = alt

    try:
        vpy, dep_rc = _create_or_update_venv(venv_dir, app_root)
        if dep_rc != 0:
            _cleanup_repo_runtime(app_root, venv_dir)
            return dep_rc
    except Exception:
        console.error(f"Failed to prepare virtual environment: {venv_dir}")
        _cleanup_repo_runtime(app_root, venv_dir)
        return 1

    child = os.environ.copy()
    child["PULSAR_VENV_ACTIVE"] = "1"
    child.setdefault("APP_ROOT", str(app_root))
    env_from_argv = _extract_env_from_argv(argv)
    if env_from_argv:
        child.setdefault("PULSAR_ENV", env_from_argv)

    _load_dotenv_into(child, app_root)

    src_dir = Path(__file__).resolve().parents[2]
    existing_pp = child.get("PYTHONPATH")
    child["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing_pp}" if existing_pp else str(src_dir)

    cmd = [str(_venv_python_path(venv_dir)), "-m", "pulsar.bootstrap", *argv]
    console.section("Repo venv launcher")
    console.info(f"Env:    {child.get('PULSAR_ENV', 'local')}")
    console.info(f"Root:   {app_root}")
    console.info(f"Venv:   {venv_dir}")

    rc = 0
    proc = None
    try:
        proc = subprocess.Popen(cmd, env=child)
        try:
            rc = int(proc.wait())
        except KeyboardInterrupt:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:
                pass
            rc = int(proc.wait())
    finally:
        _cleanup_repo_runtime(app_root, venv_dir)

    return rc
