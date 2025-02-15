from __future__ import annotations

import os
import re
import shlex
import sys
import threading
import time
import signal
from typing import Iterable, Sequence, Optional
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT

__all__ = ["console", "plsr_log"]

def _enable_windows_ansi() -> None:
    if os.name != "nt":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
            kernel32.SetConsoleMode(handle, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
    except Exception:
        pass


class _Style:
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"
    DIM = "\x1b[2m"

    FG = {
        "red": "\x1b[31m",
        "green": "\x1b[32m",
        "yellow": "\x1b[33m",
        "blue": "\x1b[34m",
        "magenta": "\x1b[35m",
        "cyan": "\x1b[36m",
        "white": "\x1b[37m",
        "gray": "\x1b[90m",
    }


def _supports_color() -> bool:
    if os.getenv("NO_COLOR"):
        return False
    if os.getenv("TERM") == "dumb":
        return False
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


_enable_windows_ansi()
_USE_COLOR = _supports_color()


def _c(text: str, color: str | None = None, *, bold: bool = False, dim: bool = False) -> str:
    if not _USE_COLOR:
        return text
    parts = []
    if bold:
        parts.append(_Style.BOLD)
    if dim:
        parts.append(_Style.DIM)
    if color:
        parts.append(_Style.FG.get(color, ""))
    parts.append(text)
    parts.append(_Style.RESET)
    return "".join(parts)


class Console:
    """
    Small, dependency-free console helper:
      - Colorized, consistent prefix: "[plsr]"
      - Section headers & ruled lines
      - Streaming command runner (merges stdout/stderr, keeps Docker progress)
      - Lightweight error highlighting and ECR auth hint
      - Graceful Ctrl+C handling for foreground processes
    """
    def __init__(self) -> None:
        self.theme = os.getenv("plsr_THEME", "neon").lower()
        if self.theme == "neon":
            self.prefix_raw = "[plsr]"
            self.prefix_colored = "\x1b[95;1m[\x1b[96;1mplsr\x1b[95;1m]\x1b[0m" if _USE_COLOR else "[plsr]"
        elif self.theme == "retro":
            self.prefix_raw = "[plsr]"
            self.prefix_colored = "\x1b[92;1m[plsr]\x1b[0m" if _USE_COLOR else "[plsr]"
        else:
            self.prefix_raw = "[plsr]"
            self.prefix_colored = _c(self.prefix_raw, "cyan", bold=True)
        self._lock = threading.Lock()


    def _out(self, msg: str) -> None:
        with self._lock:
            print(msg)

    def info(self, msg: str) -> None:
        if self.theme == 'retro':
            with self._lock:
                if _USE_COLOR:
                    print(f"\x1b[32m{self.prefix_raw} {msg}\x1b[0m")
                else:
                    print(f"{self.prefix_raw} {msg}")
            return
        self._out(f"{self.prefix_colored} {msg}")

    def success(self, msg: str) -> None:
        if self.theme == 'retro':
            with self._lock:
                if _USE_COLOR:
                    print(f"\x1b[32m{self.prefix_raw} ✔ {msg}\x1b[0m")
                else:
                    print(f"{self.prefix_raw} [SUCCESS] {msg}")
            return
        self._out(f"{self.prefix_colored} {_c('✔', 'green', bold=True)} {msg}")

    def warn(self, msg: str) -> None:
        if self.theme == 'retro':
            with self._lock:
                if _USE_COLOR:
                    print(f"\x1b[32m{self.prefix_raw} ▲ {msg}\x1b[0m")
                else:
                    print(f"{self.prefix_raw} [WARN] {msg}")
            return
        self._out(f"{self.prefix_colored} {_c('▲', 'yellow', bold=True)} {msg}")

    def error(self, msg: str) -> None:
        if self.theme == 'retro':
            with self._lock:
                if _USE_COLOR:
                    print(f"\x1b[32m{self.prefix_raw} ✖ {msg}\x1b[0m")
                else:
                    print(f"{self.prefix_raw} [ERROR] {msg}")
            return
        self._out(f"{self.prefix_colored} {_c('✖', 'red', bold=True)} {msg}")

    def tip(self, msg: str) -> None:
        if self.theme == 'retro':
            with self._lock:
                if _USE_COLOR:
                    print(f"\x1b[32m{self.prefix_raw} ➤ {msg}\x1b[0m")
                else:
                    print(f"{self.prefix_raw} [TIP] {msg}")
            return
        self._out(f"{self.prefix_colored} {_c('➤', 'magenta', bold=True)} {msg}")

    def hr(self, title: str | None = None) -> None:
        width = 80
        line = "─" * width
        if title:
            t = f" {title} "
            w = max(0, width - len(t))
            line = f"{t}{'─'*w}"
        self._out(_c(line, "gray"))

    def section(self, title: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        if self.theme == 'neon':
            width = 80
            top = "╔" + "═" * (width - 2) + "╗"
            bottom = "╚" + "═" * (width - 2) + "╝"
            text_line = f"{title} @ {ts}"
            content = "║" + text_line.center(width - 2) + "║"
            with self._lock:
                if _USE_COLOR:
                    print(f"\x1b[35m{top}\x1b[0m")
                    print(f"\x1b[96;1m{content}\x1b[0m")
                    print(f"\x1b[35m{bottom}\x1b[0m")
                else:
                    print(top)
                    print(content)
                    print(bottom)
            return
        if self.theme == 'retro':
            width = 80
            top = "+" + "-" * (width - 2) + "+"
            bottom = top
            text_line = f"{title} @ {ts}"
            content = "|" + text_line.center(width - 2) + "|"
            with self._lock:
                if _USE_COLOR:
                    print(f"\x1b[32m{top}\x1b[0m")
                    print(f"\x1b[32m{content}\x1b[0m")
                    print(f"\x1b[32m{bottom}\x1b[0m")
                else:
                    print(top)
                    print(content)
                    print(bottom)
            return
        self.hr()
        self._out(f"{self.prefix_colored} {_c(title, 'white', bold=True)} {_c('@', 'gray')} {_c(ts, 'gray')}")
        self.hr()

    @staticmethod
    def _join_cmd(cmd: Sequence[str] | str) -> str:
        if isinstance(cmd, str):
            return cmd
        try:
            return shlex.join(list(cmd))
        except Exception:
            return " ".join(cmd)

    def command(self, cmd: Sequence[str] | str, cwd: Optional[str | os.PathLike] = None) -> None:
        cmd_str = self._join_cmd(cmd)
        if cwd:
            self.info(f"Running: {_c(cmd_str, 'gray')} {_c('(cwd=' + str(cwd) + ')', 'gray', dim=True)}")
        else:
            self.info(f"Running: {_c(cmd_str, 'gray')}")

    def run(self, cmd: Sequence[str] | str, cwd: Optional[str | os.PathLike] = None, env: Optional[dict] = None) -> int:
        """
        Run a command, streaming output. Returns the exit code.
        - Merges stderr into stdout to preserve order (great for docker buildx)
        - Highlights common error lines (skip in retro theme)
        - On failure, prints an ECR login hint if we detect a 403 against ECR
        - NEW: Graceful Ctrl+C (SIGINT) handling — terminate child cleanly and
               return success (0) without printing a Python traceback.
        """
        self.command(cmd, cwd=cwd)

        proc = Popen(
            cmd if isinstance(cmd, (list, tuple)) else cmd,
            cwd=cwd,
            env=env,
            stdout=PIPE,
            stderr=STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            shell=isinstance(cmd, str),
            start_new_session=(os.name != "nt"),
        )

        recent: list[str] = []
        max_keep = 5000

        try:
            assert proc.stdout is not None
            for raw in iter(proc.stdout.readline, ""):
                line = raw.rstrip("\n")
                recent.append(line + "\n")
                while sum(len(x) for x in recent) > max_keep and recent:
                    recent.pop(0)

                out = line
                if self.theme != 'retro':
                    if "ERROR" in line or "Error:" in line or "failed to" in line.lower():
                        out = _c(line, "red")
                    elif "CACHED" in line or "DONE" in line or "FINISHED" in line:
                        out = _c(line, "green")
                    elif line.startswith("=>"):
                        out = _c(line, "blue")
                self._out(out)

            proc.wait()
            code = int(proc.returncode or 0)
        except KeyboardInterrupt:
            self.warn("Interrupted (Ctrl+C). Stopping child process…")
            try:
                if os.name != "nt":
                    try:
                        os.killpg(proc.pid, signal.SIGINT)
                    except Exception:
                        proc.send_signal(signal.SIGINT)
                else:
                    proc.send_signal(signal.CTRL_BREAK_EVENT if hasattr(signal, "CTRL_BREAK_EVENT") else signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass

            try:
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
                proc.wait(timeout=1)

            self.info("Command stopped.")
            return 0

        if code != 0:
            blob = "".join(recent)
            self._hint_if_ecr_auth(blob)
            self.error(f"Command exited with code {code}")
        else:
            self.success("Command completed")

        return code

    def _hint_if_ecr_auth(self, text: str) -> None:
        """
        Detect 403/401 on private ECR and print a useful login hint.
        """
        low = text.lower()
        if ("amazonaws.com" in text) and (
            ("403 forbidden" in low) or ("denied" in low) or ("unauthorized" in low) or ("no basic auth credentials" in low)
        ):
            m = re.search(r'([0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com)', text)
            host = m.group(1) if m else "<your-ecr-registry>"
            m2 = re.search(r'\.ecr\.([a-z0-9-]+)\.amazonaws\.com', host)
            region = m2.group(1) if m2 else "<region>"
            self.tip(_c("ECR auth hint:", "magenta", bold=True))
            self.info("Login before building if the base image is private:")
            self._out(_c(f"  aws ecr get-login-password --region {region} | docker login --username AWS --password-stdin {host}", "gray"))


    def log(self, msg: str) -> None:
        self.info(msg)


    def set_theme(self, theme: str) -> None:
        """Switch the console output theme on the fly."""
        theme = theme.lower()
        self.theme = theme
        if self.theme == "neon":
            self.prefix_raw = "[plsr]"
            self.prefix_colored = "\x1b[95;1m[\x1b[96;1mplsr\x1b[95;1m]\x1b[0m" if _USE_COLOR else "[plsr]"
        elif self.theme == "retro":
            self.prefix_raw = "[plsr]"
            self.prefix_colored = "\x1b[92;1m[plsr]\x1b[0m" if _USE_COLOR else "[plsr]"
        else:
            self.prefix_raw = "[plsr]"
            self.prefix_colored = _c(self.prefix_raw, "cyan", bold=True)

    def spinner(self, text: str = "Loading", duration: float = 3.0) -> None:
        """Display a simulated spinner animation for the specified duration."""
        spinner_chars = "|/-\\"
        end_time = time.time() + duration
        idx = 0
        with self._lock:
            while time.time() < end_time:
                char = spinner_chars[idx % len(spinner_chars)]
                if self.theme == "neon":
                    out_text = f"\x1b[95m{text} {char}\x1b[0m"
                elif self.theme == "retro":
                    out_text = f"\x1b[32m{text} {char}\x1b[0m" if _USE_COLOR else f"{text} {char}"
                else:
                    out_text = f"{text} {char}"
                sys.stdout.write("\r" + out_text)
                sys.stdout.flush()
                time.sleep(0.1)
                idx += 1
            sys.stdout.write("\r" + " " * (len(text) + 2) + "\r")
            sys.stdout.flush()

    def progress(self, total: int = 100, prefix: str = "Progress", bar_length: int = 50, fill_char: str = "█", duration: float = 5.0) -> None:
        """Display a simulated progress bar from 0% to 100%."""
        if total <= 0:
            total = 1
        interval = duration / total
        with self._lock:
            for i in range(total + 1):
                percent = int((i / total) * 100)
                filled = int(bar_length * i / total)
                bar = fill_char * filled + "-" * (bar_length - filled)
                if self.theme == "neon":
                    line = f"{prefix}: |{bar}| {percent}%"
                    line = f"\x1b[95m{line}\x1b[0m"
                elif self.theme == "retro":
                    line = f"\x1b[32m{prefix}: |{bar}| {percent}%\x1b[0m" if _USE_COLOR else f"{prefix}: |{bar}| {percent}%"
                else:
                    line = f"{prefix}: |{bar}| {percent}%"
                sys.stdout.write("\r" + line)
                sys.stdout.flush()
                time.sleep(interval)
            sys.stdout.write("\n")
            sys.stdout.flush()

console = Console()

def plsr_log(msg: str) -> None:
    """
    Back-compat alias expected by modules importing `from plsr import plsr_log`.
    """
    console.log(msg)
