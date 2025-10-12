import shutil
from pathlib import Path


def clean_path(path: str):
    """
    Delete all files and subdirectories in the given path, but keep the path itself.
    
    Args:
        path: The directory path to clean
        
    Raises:
        FileNotFoundError: If the path does not exist
        NotADirectoryError: If the path is not a directory
    """
    path_obj = Path(path)
    
    # Check if path exists
    if not path_obj.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")
    
    # Check if path is a directory
    if not path_obj.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")
    
    # Delete all contents
    for item in path_obj.iterdir():
        if item.is_file() or item.is_symlink():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)

def prepare_duckdb_sql_file(sql_file : str):
    """
    Prepare the SQL file by adding profiling configuration:
    1. Add PRAGMA enable_profiling='json' at the beginning if not present
    2. Add SET profiling_output before each SQL query statement
    """
    # Read the SQL file
    with open(sql_file, 'r') as f:
        content = f.read()

    # Split content into statements by semicolon
    # First, check if PRAGMA is present
    has_pragma = 'PRAGMA enable_profiling' in content

    # Split by semicolon to get individual statements
    statements = content.split(';')

    new_content_parts = []
    query_number = 1

    # Add PRAGMA at the beginning if not present
    if not has_pragma:
        new_content_parts.append("PRAGMA enable_profiling='json'")

    for i, statement in enumerate(statements):
        statement = statement.strip()

        # Skip empty statements
        if not statement:
            continue

        # Keep PRAGMA and existing SET statements as-is
        if statement.startswith('PRAGMA') or statement.startswith('SET'):
            new_content_parts.append(statement)
            continue

        # Check if this is a query statement (not a comment line only)
        lines = statement.split('\n')
        has_actual_sql = any(line.strip() and not line.strip().startswith('--')
                             for line in lines)

        if not has_actual_sql:
            # Just comments or whitespace, keep as-is
            new_content_parts.append(statement)
            continue

        # This is an actual SQL query
        # Check if the previous statement was a SET profiling_output
        if new_content_parts and 'SET profiling_output' in new_content_parts[-1]:
            # Already has profiling output, just add the query
            new_content_parts.append(statement)
        else:
            # Add SET profiling_output before the query
            new_content_parts.append(f"SET profiling_output='results/profiling_query_{query_number}.json'")
            new_content_parts.append(statement)

        query_number += 1

    # Reconstruct the SQL file with proper formatting
    new_content = ';\n\n'.join(new_content_parts)
    if new_content and not new_content.endswith(';'):
        new_content += ';'

    # Write back to the SQL file
    with open(sql_file, 'w') as f:
        f.write(new_content + '\n')

    print(f"✓ Prepared SQL file: {sql_file}")

def prepare_sqlite_sql_file(sql_file : str):
    """
    Prepare the SQL file for SQLite by adding .timer ON at the beginning if not present.
    """
    # Read the SQL file
    with open(sql_file, 'r') as f:
        content = f.read()

    # Check if .timer ON is present
    if '.timer on \n.stats on' not in content:
        # Add .timer ON at the beginning
        new_content = '-- Enable timer and statistics\n.timer on\n.stats on\n\n' + content
        # Write back to the SQL file
        with open(sql_file, 'w') as f:
            f.write(new_content)
        print(f"✓ Prepared SQL file: {sql_file}")
    else:
        print(f"✓ SQL file already prepared: {sql_file}")

if __name__ == "__main__":
    prepare_sqlite_sql_file("/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/queries/Q1/Q1_sqlite.sql")
    # clean_path("/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test/results")