"""
Dataset configuration data class.

This module provides the Dataset class for representing dataset configurations
with database paths for different engines.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Dataset:

    name: str
    duckdb_db: Optional[str] = None
    sqlite_db: Optional[str] = None
    chdb_db_dir: Optional[str] = None
