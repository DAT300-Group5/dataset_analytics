"""
Dataset configuration data class.

This module provides the Dataset class for representing dataset configurations
with database paths for different engines.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

# Constants for database path mapping
ENGINE_DB_MAPPING = {
    "duckdb": "duckdb_db",
    "sqlite": "sqlite_db",
    "chdb": "chdb_db_dir"
}


@dataclass
class Dataset:
    """
    Represents a dataset configuration with database paths for different engines.
    
    Attributes:
        name (str): Dataset name/identifier
        duckdb_db (Optional[str]): Path to DuckDB database file
        sqlite_db (Optional[str]): Path to SQLite database file  
        chdb_db_dir (Optional[str]): Path to ChDB database directory
    """
    name: str
    duckdb_db: Optional[str] = None
    sqlite_db: Optional[str] = None
    chdb_db_dir: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Dataset':
        """
        Create Dataset from dictionary configuration.
        
        Args:
            data: Dictionary containing dataset configuration
            
        Returns:
            Dataset: New Dataset instance
        """
        return cls(
            name=data["name"],
            duckdb_db=data.get("duckdb_db"),
            sqlite_db=data.get("sqlite_db"),
            chdb_db_dir=data.get("chdb_db_dir")
        )
    
    def get_database_path(self, engine: str) -> str:
        """
        Get database path for specific engine.
        
        Args:
            engine: Database engine name (duckdb, sqlite, chdb)
            
        Returns:
            Database path for the engine
            
        Raises:
            ValueError: If engine is not supported or path is missing
        """
        if engine not in ENGINE_DB_MAPPING:
            raise ValueError(f"Unsupported engine: {engine}")
        
        db_key = ENGINE_DB_MAPPING[engine]
        path = getattr(self, db_key.replace("_db", "_db").replace("_dir", "_db_dir"))
        
        if path is None:
            raise ValueError(f"Missing database path for engine '{engine}' in dataset '{self.name}'")
        
        return path
    
    def has_engine_support(self, engine: str) -> bool:
        """
        Check if dataset has database path configured for the specified engine.
        
        Args:
            engine: Database engine name
            
        Returns:
            True if engine is supported and path is configured
        """
        if engine not in ENGINE_DB_MAPPING:
            return False
        
        db_key = ENGINE_DB_MAPPING[engine]
        path = getattr(self, db_key.replace("_db", "_db").replace("_dir", "_db_dir"))
        return path is not None