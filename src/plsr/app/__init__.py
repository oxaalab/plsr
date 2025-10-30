import os
from pathlib import Path

from ..core.console import console

try:
    import tomllib
except ImportError:
    tomllib = None
    try:
        import tomli as tomllib
    except Exception:
        tomllib = None


def detect_app_root() -> Path:
    """
    Detect the microservice root *by project markers*, not by Git toplevel.

    Priority:
      1) APP_ROOT (if it exists)
      2) Walk up from CWD until a directory contains config.yaml or pyproject.toml
      3) Fallback to CWD
    """
    env_root = os.getenv("APP_ROOT")
    if env_root:
        p = Path(env_root).expanduser().resolve()
        if p.is_dir():
            return p

    cur = Path.cwd().resolve()
    for p in (cur, *cur.parents):
        if (p / "config.yaml").is_file() or (p / "pyproject.toml").is_file():
            return p

    return cur


def read_app_metadata(app_root: Path) -> tuple[str, str]:
    """
    Read name/version from pyproject.toml if available; else fall back
    to directory name + 0.0.0.
    """
    name = ""
    version = ""
    pp = app_root / "pyproject.toml"
    if pp.is_file() and tomllib:
        try:
            data = tomllib.loads(pp.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        proj = data.get("project") or {}
        tool = data.get("tool") or {}
        poetry = tool.get("poetry") or {}
        name = (proj.get("name") or poetry.get("name") or "").strip() or app_root.name
        version = (proj.get("version") or poetry.get("version") or "").strip() or "0.0.0"
        return name, version

    return (app_root.name, "0.0.0")


def start(env_name: str | None = None) -> None:
    """Print app identity; if env_name given, include it."""
    root = detect_app_root()
    name, ver = read_app_metadata(root)

    if os.getenv("APP_NAME"):
        name = os.getenv("APP_NAME")
    if os.getenv("APP_VERSION"):
        ver = os.getenv("APP_VERSION")

    if env_name:
        os.environ["APP_ENV"] = env_name

    console.section("Starting App")
    if env_name:
        console.info(f"Env:    {env_name}")
    console.info(f"Name:   {name}")
    console.info(f"Version:{ver}")
    console.info(f"Root:   {root}")
