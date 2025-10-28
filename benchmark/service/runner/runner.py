import subprocess
import threading
from pathlib import Path
from abc import ABC, abstractmethod
import os
import shutil
from consts.RunMode import RunMode
from util.log_config import setup_logger

logger = setup_logger(__name__)

class Runner(ABC):
    """Abstract base Runner.

    Subclasses must implement run_subprocess. Use super().__init__(...) in
    subclass constructors to initialize the common fields.
    """

    def __init__(
        self,
        sql_file: Path,
        db_file: Path,
        cmd: str,
        cwd: Path,
        run_mode: RunMode,
        results_dir: Path,
    ) -> None:
        self.sql_file = sql_file
        self.db_file = db_file
        self.cmd = cmd
        self.cwd = cwd
        self.run_mode = run_mode
        self.results_dir = results_dir
        self.temp_db_file = cwd / db_file.name

    def run_subprocess(self) -> subprocess.Popen:
        """
        Start the subprocess and return the Popen instance promptly.

        - Run `before_run()` synchronously and block until it completes.
        - Call `_run_subprocess()` to start the subprocess and obtain the
          `subprocess.Popen` instance.
        - Start a background thread that waits for the subprocess to finish,
          and calls `after_run()`. This ensures `after_run()` runs only after
          the process exits while allowing this method to return the `Popen` quickly.
        """

        # Run the preparatory steps and block until they're done.
        self.before_run()

        # Start the subprocess and get the Popen instance.
        process = self._run_subprocess()

        # Background thread: wait for process completion, log output, then run after_run().
        def _wait_and_finalize(p: subprocess.Popen) -> None:
            p.wait()
            self.after_run()

        waiter = threading.Thread(target=_wait_and_finalize, args=(process,), daemon=True)
        waiter.start()

        # Return the running process immediately to the caller.
        return process

    @abstractmethod
    def _run_subprocess(self) -> subprocess.Popen:
        """
        Start the subprocess and return the Popen instance.
        Implementations should launch the command/process for the runner and
        return the subprocess.Popen object. Raising NotImplementedError is
        replaced by @abstractmethod to enforce implementation in subclasses.
        """
        pass
    
    def before_run(self) -> None:
        
        self._copy_from_original_db()
        
        script_path = os.path.join(Path(__file__).parent / "before_run.sh")
        os.chmod(script_path, 0o755)
        result = subprocess.run(
            ["sudo", "/bin/bash", script_path],
            check=True,
            text=True,
            capture_output=False
        )
        if result.returncode != 0:
            logger.error(f"Before run script failed: {result.stderr}")
            raise RuntimeError("Before run script failed")

    def after_run(self) -> None:
        self._delete_temp_db()
    
    def _delete_temp_db(self) -> None:
        """
        Delete any existing temporary database (file or directory).
        """
        dst: Path = self.temp_db_file

        if dst.exists() or dst.is_symlink():
            try:
                if dst.is_file() or dst.is_symlink():
                    dst.unlink()
                elif dst.is_dir():
                    shutil.rmtree(dst)
                logger.info(f"Deleted old temp database: {dst}")
            except Exception as e:
                logger.error(f"Failed to delete old temp database: {e}")
    
    def _copy_from_original_db(self) -> None:
        """
        Copy a fresh database from the source database to the temporary database location.
        """
        src: Path = self.db_file
        dst: Path = self.temp_db_file
        
        try:
            # `-a` preserves attributes, symlinks, and timestamps.
            # Works for both files and directories.
            args = ["cp", "-a", str(src), str(dst)]

            # Optional: for copy-on-write (COW) file systems like Btrfs or XFS:
            # args = ["cp", "-a", "--reflink=auto", str(src), str(dst)]

            subprocess.run(args, check=True)
            logger.info(f"Copied database to temp location (cp -a): {dst}")

            # Optional: make sure all data is flushed to disk
            try:
                os.sync()
            except AttributeError:
                pass

        except subprocess.CalledProcessError as e:
            # If `cp` exits with a non-zero status, log details
            logger.error(f"`cp` failed with return code {e.returncode}. Command: {e.cmd}")
        except Exception as e:
            logger.error(f"Failed to copy database: {e}")