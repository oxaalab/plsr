"""
App (host) runtime entrypoints — compatibility shim.

We keep the canonical implementation in `pulsar.run_host` for now but expose it
under `pulsar.app` to match the new package layout. This lets the CLI and other
callers import from `pulsar.app` today, and we can physically move the code
later with no external changes.
"""

from ..run_host import auto_run, auto_stop

__all__ = ["auto_run", "auto_stop"]
