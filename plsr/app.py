import os
import subprocess
import shutil
from pathlib import Path

from plsr.console import console

try:
    import tomllib
except ImportError:
    tomllib = None
    try:
        import tomli
    except ImportError:
        tomli = None

def detect_app_root() -> Path:
    env_root = os.getenv("APP_ROOT")
    if env_root:
        p = Path(env_root)
        if p.is_dir():
            return p
    if shutil.which("git"):
        try:
            result = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                                    capture_output=True, text=True, check=True)
            git_root = result.stdout.strip()
            if git_root:
                return Path(git_root)
        except subprocess.CalledProcessError:
            pass
    current = Path.cwd()
    while True:
        if (current / "pyproject.toml").exists():
            return current
        if current.parent == current:
            break
        current = current.parent
    return Path.cwd()

def read_app_metadata(app_root: Path):
    name = ""
    version = ""
    pyproject = app_root / "pyproject.toml"
    if pyproject.is_file():
        try:
            if tomllib:
                data = tomllib.loads(pyproject.read_text())
            elif tomli:
                data = tomli.loads(pyproject.read_text())
            else:
                data = {}
        except Exception:
            data = {}
        project_data = data.get("project", {})
        name = project_data.get("name", "") or name
        version = project_data.get("version", "") or version
        tool_data = data.get("tool", {})
        poetry_data = tool_data.get("poetry", {}) if tool_data else {}
        name = name or poetry_data.get("name", "")
        version = version or poetry_data.get("version", "")
    if not name:
        name = app_root.name
    if not version:
        version = "0.0.0"
    return name, version

def start(env_name: str | None = None):
    """
    Print app identity; if env_name given (via `plsr <env> start`), include it.
    """
    root = detect_app_root()
    app_name, app_version = read_app_metadata(root)

    env_name_override = os.getenv("APP_NAME")
    env_ver_override = os.getenv("APP_VERSION")
    if env_name_override:
        app_name = env_name_override
    if env_ver_override:
        app_version = env_ver_override

    if env_name:
        os.environ["APP_ENV"] = env_name

    console.section("Starting App")
    if env_name:
        console.info(f"Env:    {env_name}")
    console.info(f"Name:   {app_name}")
    console.info(f"Version:{app_version}")
    console.info(f"Root:   {root}")
