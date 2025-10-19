from enum import Enum


class EngineType(Enum):
    SQLITE = "sqlite"
    DUCKDB = "duckdb"
    CHDB = "chdb"