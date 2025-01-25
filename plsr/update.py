import os
import shutil
import subprocess

from plsr import plsr_log

def update_lib() -> int:
    """
    Update the plsr repository by pulling the latest changes from remote.
    If PLSR is set, the update is skipped (development mode).
    Returns 0 on success, or 1 on failure.
    """
    if os.getenv("PLSR_DEV"):
        plsr_log("DEV mode detected (PLSR_DEV=1). Skipping update.")
        return 0

    repo_root = os.path.dirname(os.path.dirname(__file__))
    if not os.path.isdir(os.path.join(repo_root, ".git")):
        plsr_log(f"Not a git repository at {repo_root}; cannot update.")
        return 1
    if shutil.which("git") is None:
        plsr_log("git not found; cannot update.")
        return 1

    try:
        current_branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_root
        ).decode().strip()
    except subprocess.CalledProcessError:
        current_branch = "HEAD"
    target_branch = os.getenv("PLSR_BRANCH", current_branch)

    plsr_log(f"Updating plsr at {repo_root} (branch: {target_branch})")
    try:
        subprocess.run(["git", "fetch", "--depth", "1", "origin", target_branch], cwd=repo_root, check=True)
        subprocess.run(["git", "checkout", target_branch], cwd=repo_root, check=True)
        subprocess.run(["git", "reset", "--hard", f"origin/{target_branch}"], cwd=repo_root, check=True)
    except subprocess.CalledProcessError as e:
        plsr_log(f"Update failed: {e}")
        return 1

    return 0
