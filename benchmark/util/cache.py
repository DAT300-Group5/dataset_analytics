import os
from pathlib import Path
import shutil
import subprocess

from util.log_config import setup_logger

logger = setup_logger(__name__)


def drop_caches() -> None:
    """
    Run drop_caches.sh with root privileges if needed.
    - If running as root: run directly.
    - Else if sudo exists: run with `sudo`.
    - Else: fail with a clear error.
    """
    script_path = Path(__file__).parent / "drop_caches.sh"
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    # Prefer /usr/bin/env bash for portability; no need for +x on the script.
    cmd = ["/usr/bin/env", "bash", str(script_path)]

    # Are we root?
    is_root = False
    try:
        is_root = (os.geteuid() == 0)  # Unix-only
    except AttributeError:
        # On non-Unix (e.g., Windows), drop_caches is not supported.
        return

    if not is_root:
        sudo_path = shutil.which("sudo")
        if not sudo_path:
            raise RuntimeError(
                "Need root privileges to drop caches, but `sudo` is not installed "
                "and the current user is not root."
            )
        cmd = [sudo_path] + cmd

    try:
        # Capture stderr to print the reason on failure
        subprocess.run(cmd, check=True, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode(errors="replace") if e.stderr else ""
        logger.error(f"Failed to drop caches: {e}\nStderr: {error_output}")
        raise
    except Exception as e:
        logger.error(f"Failed to drop caches: {e}")
        raise


def delete_file(dst: Path) -> None:
    """
    Delete any existing (file or directory).
    """

    if dst.exists() or dst.is_symlink():
        try:
            if dst.is_file() or dst.is_symlink():
                dst.unlink()
            elif dst.is_dir():
                shutil.rmtree(dst)
            logger.info(f"Deleted files: {dst}")
        except Exception as e:
            logger.error(f"Failed to delete files: {e}")


def copy_file(src: Path, dst: Path) -> None:
    """
    Copy file or directory from src to dst using system `cp -a`.
    """
    
    try:
        # -a preserves attributes, symlinks, and timestamps.
        # Works for both files and directories.
        args = ["cp", "-a", str(src), str(dst)]
        # Optional: for copy-on-write (COW) file systems like Btrfs or XFS:
        # args = ["cp", "-a", "--reflink=auto", str(src), str(dst)]
        subprocess.run(args, check=True, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        # If `cp` exits with a non-zero status, log details
        stderr_output = e.stderr.decode() if e.stderr else ""
        logger.error(f"`cp` failed with return code {e.returncode}. Command: {e.cmd}\nStderr: {stderr_output}")
    except Exception as e:
        logger.error(f"Failed to copy: {e}")

    logger.info(f"Copied from {src} to {dst}")