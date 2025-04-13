from __future__ import annotations

import os
import re
import shlex
import time
import hashlib
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .console import console


def _app_root() -> Path:
    env = os.getenv("APP_ROOT")
    if env:
        p = Path(env).expanduser().resolve()
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


def _load_dotenv_map(root: Path, env_name: str) -> Dict[str, str]:
    """
    Read .env.<ENV> or env.<ENV> or .env into a dict (no external deps).
    Does not override existing OS env; just returns a mapping.
    """
    candidates = [
        root / f".env.{env_name}",
        root / f"env.{env_name}",
        root / ".env",
    ]
    out: Dict[str, str] = {}
    rx = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")
    for path in candidates:
        if not path.is_file():
            continue
        try:
            for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                if not raw or raw.lstrip().startswith("#"):
                    continue
                m = rx.match(raw)
                if not m:
                    continue
                k, v = m.group(1), m.group(2).strip()
                if len(v) >= 2 and ((v[0] == v[-1]) and v[0] in ("'", '"')):
                    v = v[1:-1]
                if k.startswith("export "):
                    k = k.split(None, 1)[-1]
                out[k] = v
            break
        except Exception:
            pass
    return out


def _env_block(text: str, env_name: str) -> str:
    envs = _extract_block(text, "environments")
    if not envs:
        return ""
    m = re.search(rf"(?ms)^\s*{re.escape(env_name)}\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", envs)
    return m.group("blk") if m else ""


def _db_url_from_config(text: str, env_name: str, root: Path) -> Optional[str]:
    ev = os.getenv("DATABASE_URL")
    if ev and ev.strip():
        return ev.strip()

    env_map = _load_dotenv_map(root, env_name)
    if env_map.get("DATABASE_URL"):
        return env_map["DATABASE_URL"].strip()

    blk = _env_block(text, env_name)
    if blk:
        v = _kv_from_block(blk, "DATABASE_URL")
        if v:
            return v.strip()

    default_blk = _extract_block(text, "default")
    v = _kv_from_block(default_blk, "DATABASE_URL")
    if v:
        return v.strip()

    v = _kv_top(text, "DATABASE_URL")
    if v:
        return v.strip()

    return None


def _root_password(text: str, env_name: str) -> str:
    """
    Return DB root password from config if provided, otherwise 'Password1'.
    - environments.<env>.root_pw
    - dev.root.password (legacy fallback)
    """
    blk = _env_block(text, env_name)
    v = _kv_from_block(blk, "root_pw") if blk else None
    if v:
        return v.strip()

    dev_blk = _extract_block(text, "dev")
    if dev_blk:
        m = re.search(r"(?ms)^\s*root\s*:\s*\n(?P<blk>(?:[ \t].*\n)+)", dev_blk)
        sub = m.group("blk") if m else ""
        v = _kv_from_block(sub, "password")
        if v:
            return v.strip()

    return "Password1"


@dataclass
class MySQLConn:
    host: str
    port: int
    user: str
    password: str
    db: str


def _mysql_bin() -> Optional[str]:
    for cand in ("mysql", "mariadb"):
        path = shutil.which(cand)
        if path:
            return path
    return None


def _parse_mysql_like(url: str) -> MySQLConn:
    """
    Accepts mysql://, mysql+pymysql://, mysql+asyncmy://, etc.
    """

    if "://" not in url:
        raise ValueError("missing scheme")
    scheme, rest = url.split("://", 1)
    base = scheme.split("+", 1)[0]
    if base != "mysql":
        raise ValueError(f"unsupported scheme '{scheme}'")

    from urllib.parse import urlparse, unquote

    parsed = urlparse(f"{base}://{rest}")
    user = unquote(parsed.username or "")
    pwd = unquote(parsed.password or "")
    host = parsed.hostname or "127.0.0.1"
    port = int(parsed.port or 3306)
    db = (parsed.path or "/").lstrip("/") or "mysql"
    return MySQLConn(host=host, port=port, user=user, password=pwd, db=db)


def _cmd_base(conn: MySQLConn, *, use_db: bool = False) -> str:
    bin_ = _mysql_bin() or "mysql"
    parts = [shlex.quote(bin_), "-h", shlex.quote(conn.host), "-P", str(conn.port), "-u", shlex.quote(conn.user)]
    if conn.password:
        parts.append(f"--password={shlex.quote(conn.password)}")
    if use_db and conn.db:
        parts.extend(["-D", shlex.quote(conn.db)])
    return " ".join(parts)


def _run(cmd: str) -> Tuple[int, str]:
    """
    Run a shell command (we need shell for input redirection).
    """
    console.command(cmd)
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    out = (res.stdout or "") + (res.stderr or "")
    if res.returncode != 0:
        console.error(out.strip() or f"Command failed: {cmd}")
    return int(res.returncode or 0), out


def _wait_for_db(conn: MySQLConn, *, attempts: int = 60, delay: float = 1.0) -> bool:
    ping = _cmd_base(conn) + " -e " + shlex.quote("SELECT 1;")
    for _ in range(max(1, attempts)):
        code, _ = _run(ping)
        if code == 0:
            return True
        time.sleep(delay)
    return False


def _exec_sql(conn: MySQLConn, sql: str, *, use_db: bool = False) -> Tuple[int, str]:
    cmd = _cmd_base(conn, use_db=use_db) + " -e " + shlex.quote(sql)
    return _run(cmd)


def _exec_sql_file(conn: MySQLConn, path: Path, *, use_db: bool = False) -> Tuple[int, str]:
    cmd = _cmd_base(conn, use_db=use_db) + " < " + shlex.quote(str(path))
    return _run(cmd)


_LEDGER_TABLE = "schema_migrations"

def _ensure_ledger(conn: MySQLConn) -> int:
    sql = f"""
        CREATE TABLE IF NOT EXISTS `{_LEDGER_TABLE}` (
          `name` VARCHAR(255) NOT NULL PRIMARY KEY,
          `checksum` CHAR(64) NOT NULL,
          `applied_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    code, _ = _exec_sql(conn, sql, use_db=True)
    return code


def _list_sql_files(db_dir: Path) -> List[Path]:
    return sorted([p for p in db_dir.glob("*.sql") if p.is_file()], key=lambda x: x.name)


def _file_checksum(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def _already_applied(conn: MySQLConn) -> Dict[str, str]:
    code, out = _exec_sql(conn, f"SELECT name, checksum FROM `{_LEDGER_TABLE}`;", use_db=True)
    if code != 0:
        return {}
    rows = [ln.strip().split("\t") for ln in out.strip().splitlines() if ln.strip()]
    if rows and rows[0] and rows[0][0].lower() == "name":
        rows = rows[1:]
    return {name: cks for name, cks in rows if name}


def _insert_ledger(conn: MySQLConn, name: str, checksum: str) -> int:
    esc_name = name.replace("'", "''")
    esc_ck = checksum.replace("'", "''")
    sql = f"INSERT INTO `{_LEDGER_TABLE}` (`name`, `checksum`) VALUES ('{esc_name}', '{esc_ck}');"
    code, _ = _exec_sql(conn, sql, use_db=True)
    return code


def _ensure_db_and_user(root_conn: MySQLConn, svc_conn: MySQLConn) -> int:
    db = svc_conn.db.replace("`", "``")
    usr = svc_conn.user.replace("`", "``")
    pw = svc_conn.password.replace("'", "''")
    stmts: List[str] = [
        f"CREATE DATABASE IF NOT EXISTS `{db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
        f"CREATE USER IF NOT EXISTS '{usr}'@'%' IDENTIFIED BY '{pw}';",
        f"GRANT ALL ON `{db}`.* TO '{usr}'@'%';",
        "FLUSH PRIVILEGES;",
    ]
    code, _ = _exec_sql(root_conn, " ".join(stmts), use_db=False)
    return code



def migrate(env_name: str) -> int:
    """
    Apply db/*.sql in order to the database pointed to by DATABASE_URL
    (discovered from env/.env/config.yaml) for <env_name>.
    """
    if not env_name:
        console.error("Environment name is required. Use: ctl.sh db migrate <env>")
        return 2

    if not _mysql_bin():
        console.error("MySQL client not found (mysql/mariadb).")
        console.tip("macOS (Homebrew):  brew install mysql-client   and ensure it is on PATH (brew info mysql-client).")
        return 127

    root = _app_root()
    cfg = _read_config_text(root)
    if not cfg:
        console.error(f"No config.yaml found under {root}")
        return 2

    url = _db_url_from_config(cfg, env_name, root)
    if not url:
        console.error("DATABASE_URL is required (in environments.<env>, default, .env, or env var).")
        console.tip(f"Searched: {root/'config.yaml'} and .env files")
        return 2

    try:
        svc_conn = _parse_mysql_like(url)
    except Exception as e:
        console.error(f"Invalid DATABASE_URL: {e}")
        return 2

    console.section("DB Migration")
    console.info(f"Env:    {env_name}")
    console.info(f"Target: {svc_conn.user}@{svc_conn.host}:{svc_conn.port}/{svc_conn.db}")


    root_conn = MySQLConn(
        host=svc_conn.host,
        port=svc_conn.port,
        user="root",
        password=_root_password(cfg, env_name),
        db="mysql",
    )

    console.info("Waiting for DB to become ready…")
    if not _wait_for_db(root_conn, attempts=60, delay=1.0):
        console.error("Database is not reachable at the configured host/port.")
        console.tip("Ensure the DB microservice is running (e.g., svc-db) and ports match your DATABASE_URL.")
        return 1

    rc = _ensure_db_and_user(root_conn, svc_conn)
    if rc != 0:
        return rc

    rc = _ensure_ledger(svc_conn)
    if rc != 0:
        return rc

    db_dir = root / "db"
    if not db_dir.is_dir():
        console.warn(f"No 'db/' directory at {db_dir}. Nothing to migrate.")
        return 0

    files = _list_sql_files(db_dir)
    if not files:
        console.warn("No SQL files found under db/. Nothing to migrate.")
        return 0

    applied = _already_applied(svc_conn)

    for path in files:
        name = path.name
        checksum = _file_checksum(path)

        if applied.get(name) == checksum:
            console.info(f"✓ {name} (already applied)")
            continue

        if name in applied and applied[name] != checksum:
            console.error(f"Refusing to re-apply modified migration: {name}")
            console.tip("Create a new numbered migration file instead.")
            return 1

        console.info(f"→ Applying {name} …")
        code, out = _exec_sql_file(svc_conn, path, use_db=True)
        if code != 0:
            console.error(out if out else f"Migration failed: {name}")
            return code

        rec = _insert_ledger(svc_conn, name, checksum)
        if rec != 0:
            console.error(f"Failed to record migration in ledger: {name}")
            return rec

        console.success(f"Applied {name}")

    console.success("All migrations are up to date.")
    return 0
