"""
Query group configuration data class.

This module provides the QueryGroup class for representing query group configurations
with SQL file paths for different engines.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class QueryGroup:

    id: str
    duckdb_sql: Optional[str] = None
    sqlite_sql: Optional[str] = None
    chdb_sql: Optional[str] = None
    duckdb_sql_ban_ops: Optional[str] = None
    chdb_sql_ban_ops: Optional[str] = None
    sqlite_sql_ban_ops: Optional[str] = None
