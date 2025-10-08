"""
Configuration file for SQLite Demo Runner

You can customize the settings here instead of passing command line arguments.
"""

# SQLite3 executable path
# Examples:
#   - Default (use system sqlite3): "sqlite3"
#   - Custom compiled version: "/Users/admin/sqlite3/bin/sqlite3"
#   - Homebrew installation: "/usr/local/bin/sqlite3"
#   - MacPorts installation: "/opt/local/bin/sqlite3"
SQLITE_CMD = "sqlite3"

# File paths
SQL_FILE = "demo.sql"
DB_FILE = "demo.db"
OUTPUT_LOG = "results/output.log"
JSON_OUTPUT = "results.json"

# Execution settings
SAVE_JSON = True
EXECUTION_TIMEOUT = 300  # seconds (5 minutes)
