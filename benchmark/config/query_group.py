"""
Query group configuration data class.

This module provides the QueryGroup class for representing query group configurations
with SQL file paths for different engines.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

# Constants for SQL path mapping
ENGINE_SQL_MAPPING = {
    "duckdb": "duckdb_sql",
    "sqlite": "sqlite_sql",
    "chdb": "chdb_sql"
}


@dataclass  
class QueryGroup:
    """
    Represents a query group configuration with SQL paths for different engines.
    
    Attributes:
        id (str): Query group identifier
        duckdb_sql (Optional[str]): Path to DuckDB SQL file
        sqlite_sql (Optional[str]): Path to SQLite SQL file
        chdb_sql (Optional[str]): Path to ChDB SQL file
    """
    id: str
    duckdb_sql: Optional[str] = None
    sqlite_sql: Optional[str] = None
    chdb_sql: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryGroup':
        """
        Create QueryGroup from dictionary configuration.
        
        Args:
            data: Dictionary containing query group configuration
            
        Returns:
            QueryGroup: New QueryGroup instance
        """
        return cls(
            id=data["id"],
            duckdb_sql=data.get("duckdb_sql"),
            sqlite_sql=data.get("sqlite_sql"),
            chdb_sql=data.get("chdb_sql")
        )
    
    def get_sql_path(self, engine: str) -> str:
        """
        Get SQL path for specific engine.
        
        Args:
            engine: Database engine name (duckdb, sqlite, chdb)
            
        Returns:
            SQL file path for the engine
            
        Raises:
            ValueError: If engine is not supported or path is missing
        """
        if engine not in ENGINE_SQL_MAPPING:
            raise ValueError(f"Unsupported engine: {engine}")
        
        sql_key = ENGINE_SQL_MAPPING[engine]
        path = getattr(self, sql_key)
        
        if path is None:
            raise ValueError(f"Missing SQL path for engine '{engine}' in query group '{self.id}'")
        
        return path
    
    def has_engine_support(self, engine: str) -> bool:
        """
        Check if query group has SQL path configured for the specified engine.
        
        Args:
            engine: Database engine name
            
        Returns:
            True if engine is supported and SQL path is configured
        """
        if engine not in ENGINE_SQL_MAPPING:
            return False
        
        sql_key = ENGINE_SQL_MAPPING[engine]
        path = getattr(self, sql_key)
        return path is not None