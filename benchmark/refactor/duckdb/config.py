"""
Configuration for DuckDB benchmarking system
"""

# Path to the DuckDB command-line tool
# Can be overridden via command-line argument
DUCKDB_CMD = "duckdb"

# Database file path (will be created if it doesn't exist)
DB_FILE = "demo.db"

# SQL script file path
SQL_FILE = "demo.sql"

# Profiling JSON output file path (fallback, actual files in results/ directory)
PROFILING_OUTPUT = "results/profiling_output.json"

# Results JSON file path (will be saved in results/ directory)
RESULTS_FILE = "results.json"

# Process monitoring settings
PROCESS_MONITOR_INTERVAL = 0.1  # seconds (100ms)
