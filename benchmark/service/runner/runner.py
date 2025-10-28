import subprocess
from pathlib import Path
from abc import ABC, abstractmethod
import os
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

    def run_subprocess(self) -> subprocess.Popen:
        """
        Start the subprocess and return the Popen instance of the main running process.
        
        This method calls before_run() before starting the subprocess and after_run() after the subprocess is started.
        """
        
        self.before_run()
        
        process = self._run_subprocess()
        
        self.after_run()

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
        pass
