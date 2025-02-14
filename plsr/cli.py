import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from plsr import k8s, app as app_module
from plsr.console import console
from plsr import run_local
from plsr import run_host
from plsr.build import docker_build_from_config, print_image_tag_from_config
from plsr import db_migrate
from plsr import helm_release


def _print_help_and_exit(parser):
    parser.print_help(sys.stdout)
    sys.exit(0)


def _consume_global_flags(argv: List[str]) -> Tuple[bool, str | None, List[str]]:
    """
    Extract global flags (-dev/--dev and --theme <name>) from argv no matter where
    they are placed, and return (dev_mode, theme, remaining_argv).
    """
    dev = False
    theme = None
    out: List[str] = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-dev", "--dev"):
            dev = True
            i += 1
            continue
        if a == "--theme" and i + 1 < len(argv):
            theme = argv[i + 1]
            i += 2
            continue
        out.append(a)
        i += 1
    return dev, theme, out


def _require_env_hint(cmd_stub: str) -> int:
    """
    Print a consistent, user-friendly hint when ENV is missing.
    """
    console.error("Environment name is required.")
    console.tip(f"Use: plsr {cmd_stub} <env>")
    console.info("Examples:")
    console.info(f"  ./ctl.sh {cmd_stub} local")
    console.info(f"  plsr {cmd_stub} dev")
    return 2

def update_lib(dev_mode: bool):
    if dev_mode or os.getenv("plsr_DEV"):
        console.info("DEV mode detected (plsr_DEV=1). Skipping update.")
        return 0

    repo_root = Path(__file__).resolve().parent.parent
    if not (repo_root / ".git").is_dir():
        console.warn(f"Not a git repository at {repo_root}; cannot update.")
        return 1
    if shutil.which("git") is None:
        console.error("git not found; cannot update.")
        return 1

    try:
        res = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, check=True
        )
        current_branch = res.stdout.strip()
    except subprocess.CalledProcessError:
        current_branch = None

    target_branch = os.getenv("plsr_BRANCH") or current_branch or "main"
    console.info(f"Updating plsr at {repo_root} (branch: {target_branch})")
    try:
        code = 0
        code |= console.run(["git", "-C", str(repo_root), "fetch", "--depth", "1", "origin", target_branch])
        if code != 0:
            return 1
        code |= console.run(["git", "-C", str(repo_root), "checkout", target_branch])
        if code != 0:
            return 1
        code |= console.run(["git", "-C", str(repo_root), "reset", "--hard", f"origin/{target_branch}"])
        return 0 if code == 0 else 1
    except subprocess.CalledProcessError as e:
        console.error(f"Update failed: {e}")
        return 1

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="plsr",
        description="plsr CLI – central orchestrator for microservices (local dev workflows)"
    )
    parser.add_argument("-dev", "--dev", action="store_true", help="Development mode (skip self-update)")
    parser.add_argument("--theme", choices=["current", "neon", "retro"], help="Choose console output theme")

    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("hello", help="Print 'hello world'")

    subparsers.add_parser("update-lib", help="Update plsr library (skips in -dev)")

    k8s_parser = subparsers.add_parser("k8s", help="Kubernetes helper commands")
    k8s_sub = k8s_parser.add_subparsers(dest="k8s_cmd")
    k8s_sub.add_parser("setup", help="Install ingress + cert-manager + ClusterIssuer")
    k8s_sub.add_parser("install", help="Alias for 'setup'")
    k8s_sub.add_parser("install-ingress", help="Install only NGINX Ingress")
    k8s_sub.add_parser("ingress", help="Alias for 'install-ingress'")
    k8s_sub.add_parser("cluster-issuer", help="Apply ClusterIssuer.yaml")
    k8s_sub.add_parser("issuer", help="Alias for 'cluster-issuer'")

    app_parser = subparsers.add_parser("app", help="App lifecycle commands")
    app_sub = app_parser.add_subparsers(dest="app_cmd")
    app_sub.add_parser("start", help="Print detected app name and version")

    docker_parser = subparsers.add_parser("docker", help="Docker helpers")
    docker_sub = docker_parser.add_subparsers(dest="docker_cmd")

    docker_build_p = docker_sub.add_parser("build", help="Build image using config.yaml (buildx)")
    docker_build_p.add_argument("env", help="Environment name (positional, required)")
    docker_build_p.add_argument("--context", default=".", help="Build context (default: .)")
    docker_build_p.add_argument("-f", "--dockerfile", default=None, help="Path to Dockerfile")
    docker_build_p.add_argument("--platform", default="linux/amd64", help="Target platform")
    docker_build_p.add_argument("--push", action="store_true", help="Push image to registry (non-DB only)")
    docker_build_p.add_argument("--target", default=None, help="Multi-stage target")
    docker_build_p.add_argument("--no-cache", action="store_true", help="Disable build cache")
    docker_build_p.add_argument("--build-arg", action="append", default=[], help="--build-arg KEY=VALUE")
    docker_build_p.add_argument("--label", action="append", default=[], help="--label KEY=VALUE")
    docker_build_p.add_argument("--no-preflight", action="store_true", help="Skip AWS/ECR preflight")
    docker_build_p.add_argument("--no-prepull", action="store_true", help="Skip pre-pulling private ECR bases")

    docker_sub.add_parser("print-tag", help="Print computed image tag from config.yaml")

    docker_run_p = docker_sub.add_parser("run", help="Run the microservice as a Docker container")
    docker_run_p.add_argument("env", help="Environment name (positional, required)")
    docker_run_p.add_argument("--image", default=None, help="Override image to run")
    docker_run_p.add_argument("--name", default=None, help="Override container name")
    docker_run_p.add_argument("-p", "--port", action="append", default=[], help="Port mapping(s) HOST:CONT")
    docker_run_p.add_argument("--env-var", "-E", action="append", default=[], help="Container env KEY=VALUE")
    docker_run_p.add_argument("--data-dir", default=None, help="Host dir for DB persistent data")
    docker_run_p.add_argument("--no-detach", action="store_true", help="Run in foreground")
    docker_run_p.add_argument("--pull", action="store_true", help="Force docker pull before run")
    docker_run_p.add_argument("--skip-aws", action="store_true", help="Skip AWS/ECR login when pulling image")
    docker_run_p.add_argument("--dry-run", action="store_true", help="Print docker run command without executing")

    docker_stop_p = docker_sub.add_parser("stop", help="Stop/remove the microservice Docker container")
    docker_stop_p.add_argument("env", help="Environment name (positional, required)")
    docker_stop_p.add_argument("--name", default=None, help="Override container name")
    docker_stop_p.add_argument("--keep-data", action="store_true", help="Keep DB data dir (default: delete)")
    docker_stop_p.add_argument("--dry-run", action="store_true", help="Print actions without executing")

    host_run_p = subparsers.add_parser("run", help="Run the service directly on the host (no Docker)")
    host_run_p.add_argument("env", help="Environment name (positional, required)")
    host_run_p.add_argument("--dry-run", action="store_true", help="Preview host runtime actions")

    host_stop_p = subparsers.add_parser("stop", help="Stop the host (non-Docker) runtime")
    host_stop_p.add_argument("env", help="Environment name (positional, required)")
    host_stop_p.add_argument("--dry-run", action="store_true", help="Preview host runtime actions")

    build_top = subparsers.add_parser("build", help="Alias for 'docker build'")
    build_top.add_argument("env", help="Environment name (positional, required)")

    start_top = subparsers.add_parser("start", help="Start the service on the host (no Docker)")
    start_top.add_argument("env", nargs="?", default=None, help="Environment name (default: local)")

    db_parser = subparsers.add_parser("db", help="Database helpers (schema migrations)")
    db_sub = db_parser.add_subparsers(dest="db_cmd")
    db_migrate_p = db_sub.add_parser("migrate", help="Apply db/*.sql migrations")
    db_migrate_p.add_argument("env", help="Environment name (positional, required)")

    helm_parser = subparsers.add_parser("helm", help="Helm-based deploys via the central plsr chart")
    helm_sub = helm_parser.add_subparsers(dest="helm_cmd")
    helm_release_p = helm_sub.add_parser("release", help="Release/upgrade the service into an env namespace")
    helm_release_p.add_argument("env", help="Environment name (positional, required)")

    return parser

def _env_run_parser(env_name: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=f"plsr {env_name} run",
        description=f"Run the service on host for environment '{env_name}' (no Docker)"
    )
    p.add_argument("--dry-run", action="store_true", help="Preview host runtime actions")
    return p

def _env_stop_parser(env_name: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=f"plsr {env_name} stop",
        description=f"Stop the host runtime for environment '{env_name}' (no Docker)"
    )
    p.add_argument("--dry-run", action="store_true", help="Preview host runtime actions")
    return p

def _env_db_parser(env_name: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=f"plsr {env_name} db migrate",
        description=f"Apply db/*.sql migrations for environment '{env_name}'"
    )
    return p

def _env_helm_parser(env_name: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=f"plsr {env_name} helm release",
        description=f"Release/upgrade the service into Kubernetes for environment '{env_name}'"
    )
    return p

def run_from_args(argv=None) -> int:
    argv = list(argv or sys.argv[1:])

    dev_mode, theme, argv = _consume_global_flags(argv)
    if dev_mode:
        os.environ["plsr_DEV"] = "1"
    if theme:
        console.set_theme(theme)

    KNOWN_TOP = {"hello", "update-lib", "k8s", "app", "docker", "build", "start", "run", "stop", "db", "helm"}
    if argv and argv[0] not in KNOWN_TOP:
        env = argv[0]
        if len(argv) == 1:
            console.error("Missing subcommand. Usage: plsr <env> (run|stop|start|db migrate|helm release) [options]")
            return 2
        sub = argv[1]
        rest = argv[2:]

        os.environ["plsr_ENV"] = env

        if sub == "run":
            parser = _env_run_parser(env)
            a, _unknown = parser.parse_known_args(rest)
            return run_host.auto_run(env_name=env, dry_run=bool(a.dry_run))

        if sub == "stop":
            parser = _env_stop_parser(env)
            a, _unknown = parser.parse_known_args(rest)
            return run_host.auto_stop(env_name=env, dry_run=bool(a.dry_run))

        if sub == "start":
            app_module.start(env_name=env)
            return run_host.auto_run(env_name=env)

        if sub == "db":
            if not rest:
                console.error("Missing db action. Use: migrate")
                return 2
            action = rest[0]
            if action == "migrate":
                return db_migrate.migrate(env_name=env)
            console.error(f"Unknown db action '{action}'. Supported: migrate")
            return 2

        if sub == "helm":
            if not rest:
                console.error("Missing helm action. Use: release")
                return 2
            action = rest[0]
            if action == "release":
                _env_helm_parser(env).parse_known_args(rest[1:])
                return helm_release.release(env_name=env)
            console.error(f"Unknown helm action '{action}'. Supported: release")
            return 2

        console.error(f"Unknown microservice subcommand '{sub}'. Use: run | stop | start | db migrate | helm release")
        return 2

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        _print_help_and_exit(parser)

    if args.command == "hello":
        print("hello world")
        return 0

    if args.command == "update-lib":
        return update_lib(dev_mode)

    if args.command == "k8s":
        if args.k8s_cmd in (None, "setup", "install"):
            k8s.setup(); return 0
        if args.k8s_cmd in ("install-ingress", "ingress"):
            k8s.install_ingress(); return 0
        if args.k8s_cmd in ("cluster-issuer", "issuer"):
            k8s.apply_clusterissuer(); return 0
        build_parser().parse_args(["k8s", "-h"]); return 1

    if args.command == "app":
        if args.app_cmd in (None, "start"):
            app_module.start(); return 0
        build_parser().parse_args(["app", "-h"]); return 1

    if args.command == "docker":
        if args.docker_cmd == "build":
            env_name = args.env
            if not env_name:
                return _require_env_hint("docker build")
            os.environ["plsr_ENV"] = env_name
            return docker_build_from_config(
                context=args.context,
                dockerfile=args.dockerfile,
                platform=args.platform,
                push=bool(args.push),
                target=args.target,
                no_cache=bool(args.no_cache),
                build_args=list(args.build_arg or []),
                labels=list(args.label or []),
                preflight=True if not args.no_preflight else False,
                pre_pull=True if not args.no_prepull else False,
            )
        if args.docker_cmd == "print-tag":
            return print_image_tag_from_config()
        if args.docker_cmd == "run":
            env_name = args.env
            if not env_name:
                return _require_env_hint("docker run")
            return run_local.auto_run(
                env_name=env_name,
                image_override=args.image,
                name_override=args.name,
                port_overrides=list(args.port or []),
                env_overrides=list(args.env_var or []),
                data_host_dir=args.data_dir,
                detach=not bool(args.no_detach),
                force_pull=bool(args.pull),
                skip_aws=bool(args.skip_aws),
                dry_run=bool(args.dry_run),
            )
        if args.docker_cmd == "stop":
            env_name = args.env
            if not env_name:
                return _require_env_hint("docker stop")
            return run_local.auto_stop(
                env_name=env_name,
                name_override=args.name,
                keep_data=bool(args.keep_data),
                dry_run=bool(args.dry_run),
            )
        build_parser().parse_args(["docker", "-h"]); return 1

    if args.command == "run":
        env_name = args.env
        if not env_name:
            return _require_env_hint("run")
        return run_host.auto_run(env_name=env_name, dry_run=bool(args.dry_run))

    if args.command == "stop":
        env_name = args.env
        if not env_name:
            return _require_env_hint("stop")
        return run_host.auto_stop(env_name=env_name, dry_run=bool(args.dry_run))

    if args.command == "build":
        env_name = args.env
        if not env_name:
            return _require_env_hint("build")
        os.environ["plsr_ENV"] = env_name
        return docker_build_from_config()

    if args.command == "start":
        env_name = args.env or os.getenv("plsr_ENV") or "local"
        os.environ["plsr_ENV"] = env_name
        app_module.start(env_name=env_name)
        return run_host.auto_run(env_name=env_name)

    if args.command == "db":
        if args.db_cmd == "migrate":
            env_name = args.env
            if not env_name:
                return _require_env_hint("db migrate")
            return db_migrate.migrate(env_name=env_name)
        build_parser().parse_args(["db", "-h"]); return 1

    if args.command == "helm":
        if args.helm_cmd == "release":
            env_name = args.env
            if not env_name:
                return _require_env_hint("helm release")
            return helm_release.release(env_name=env_name)
        build_parser().parse_args(["helm", "-h"]); return 1

    _print_help_and_exit(parser)
    return 1


def main():
    sys.exit(run_from_args())
