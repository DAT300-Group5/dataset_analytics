import sqlite3
import duckdb
import os

def _load_query_from_file(query_or_path):
    """
    Helper function to load SQL query from file or return the query string directly.
    
    Args:
        query_or_path (str): SQL query string or path to SQL file
        
    Returns:
        str: SQL query string
        
    Raises:
        FileNotFoundError: If file path is provided but file doesn't exist
        IOError: If file cannot be read
    """
    if not query_or_path or not query_or_path.strip():
        return query_or_path
    
    query_stripped = query_or_path.strip()
    
    # Check if it looks like a SQL query (contains SQL keywords)
    sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
    if any(keyword in query_stripped.upper() for keyword in sql_keywords):
        # It's likely a SQL query, return it directly
        return query_stripped
    
    # Check if it's a file path (contains file extension or exists as file)
    if (query_stripped.endswith('.sql') or 
        os.path.exists(query_stripped) or 
        '/' in query_stripped or '\\' in query_stripped):
        
        try:
            with open(query_stripped, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except FileNotFoundError:
            raise FileNotFoundError(f"SQL file not found: {query_stripped}")
        except IOError as e:
            raise IOError(f"Error reading SQL file {query_stripped}: {e}")
    
    # Otherwise, treat it as a direct SQL query string
    return query_stripped

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
        raise ValueError("Query cannot be empty")
    
    # Load SQL query from file or use directly
    sql_query = _load_query_from_file(query)
    
    if not sql_query or not sql_query.strip():
        raise ValueError("SQL query cannot be empty")
        
    if db_path is None:
        db_path = './data.sqlite'

    try:
        # Use context manager to ensure connection is properly closed
        with sqlite3.connect(db_path) as con:
            result = con.execute(sql_query).fetchall()
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
    
    # Load SQL query from file or use directly
    sql_query = _load_query_from_file(query)
    
    if not sql_query or not sql_query.strip():
        raise ValueError("SQL query cannot be empty")
        
    if db_path is None:
        db_path = './data.duckdb'
    
    try:
        # Use context manager to ensure connection is properly closed
        with duckdb.connect(db_path) as con:
            result = con.execute(sql_query).fetchall()
            return len(result) if result is not None else 0
    except Exception as e:  # DuckDB uses generic Exception for errors
        raise Exception(f"DuckDB error: {e}")