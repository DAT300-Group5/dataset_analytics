import subprocess
import threading
from pathlib import Path
from abc import ABC, abstractmethod
import os
import shutil
from consts.RunMode import RunMode
from util.cache import copy_file, delete_file, drop_caches
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

    @abstractmethod
    def run_subprocess(self) -> subprocess.Popen:
        """
        Start the subprocess and return the Popen instance.
        Implementations should launch the command/process for the runner and
        return the subprocess.Popen object. Raising NotImplementedError is
        replaced by @abstractmethod to enforce implementation in subclasses.
        """
        pass
    
    def before_run(self) -> None:
        copy_file(self.db_file, self.temp_db_file)
        drop_caches()

    def after_run(self) -> None:
        delete_file(self.temp_db_file)