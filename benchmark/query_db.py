import sqlite3
import duckdb

def run_query(engine='duckdb', query='', db_path=None):
    """
    Run query according to the specified engine
    """

    if engine.lower() == 'duckdb':
        return run_query_duckdb(query, db_path)
    elif engine.lower() == 'sqlite':
        return run_query_sqlite(query, db_path)
    else:
        raise ValueError(f"Unsupported database engine: {engine}. Supported engines: duckdb, sqlite.")

# RUN QUERY ON SQLITE DB ================================
def run_query_sqlite(query, db_path=None):
    """
    Execute SQLite query and return the number of rows.
    
    Args:
        query (str): SQL query string to execute OR path to SQL file
        db_path (str, optional): Path to SQLite database file. Defaults to './data.sqlite'
    
    Returns:
        int: Number of rows returned by the query
        
    Raises:
        sqlite3.Error: If database operation fails
        ValueError: If query is empty or None
        FileNotFoundError: If SQL file is not found
        IOError: If SQL file cannot be read
    """
    
    if not query or not query.strip():
        raise ValueError("SQL query cannot be empty")
        
    if db_path is None:
        db_path = './data.sqlite'

    try:
        # Use context manager to ensure connection is properly closed
        with sqlite3.connect(db_path) as con:
            result = con.execute(query).fetchall()
            return len(result) if result is not None else 0
    except sqlite3.Error as e:
        raise sqlite3.Error(f"SQLite error: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error: {e}")


# RUN QUERY ON DUCKDB DB ==============================
def run_query_duckdb(query, db_path=None):
    """
    Execute DuckDB query and return the number of rows.
    
    Args:
        query (str): SQL query string to execute OR path to SQL file
        db_path (str, optional): Path to DuckDB database file. Defaults to './data.duckdb'
    
    Returns:
        int: Number of rows returned by the query
        
    Raises:
        duckdb.Error: If database operation fails
        ValueError: If query is empty or None
        FileNotFoundError: If SQL file is not found
        IOError: If SQL file cannot be read
    """
    if not query or not query.strip():
        raise ValueError("Query cannot be empty")
        
    if db_path is None:
        db_path = './data.duckdb'
    
    try:
        # Use context manager to ensure connection is properly closed
        with duckdb.connect(db_path) as con:
            result = con.execute(query).fetchall()
            return len(result) if result is not None else 0
    except Exception as e:  # DuckDB uses generic Exception for errors
        raise Exception(f"DuckDB error: {e}")