import subprocess
from pathlib import Path
from abc import ABC, abstractmethod

from consts.RunMode import RunMode


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

    @abstractmethod
    def run_subprocess(self) -> subprocess.Popen:
        """Start the subprocess and return the Popen instance.

        Implementations should launch the command/process for the runner and
        return the subprocess.Popen object. Raising NotImplementedError is
        replaced by @abstractmethod to enforce implementation in subclasses.
        """
        pass
