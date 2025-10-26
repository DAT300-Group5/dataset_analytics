import shutil
from pathlib import Path


def resolve_cmd(cmd: str) -> str:
    p = Path(cmd)
    if p.is_file() or ("/" in cmd or "\\" in cmd):
        return str(p.resolve())
    found = shutil.which(cmd)
    if found:
        return found
    raise FileNotFoundError(
        f"Executable '{cmd}' not found. "
        f"Either provide a path (e.g. './duckdb') or ensure it's in PATH."
    )


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


def project_root(start: Path | None = None) -> Path:
    """
    Find the nearest ancestor directory (including the start directory) that
    contains a '.git' folder. This strictly identifies the Git repository root.

    Args:
        start: Optional starting path to search from. If a file path is provided,
               its parent directory is used. Defaults to this file's directory.

    Returns:
        Path to the detected Git repository root.

    Raises:
        FileNotFoundError: If no directory containing a '.git' folder is found
                           from the start path up to the filesystem root.
    """
    start_path = (start or Path(__file__)).resolve()
    start_dir = start_path if start_path.is_dir() else start_path.parent

    # Walk upwards from start_dir to filesystem root
    for candidate in [start_dir] + list(start_dir.parents):
        try:
            if (candidate / ".git").is_dir():
                return candidate
        except Exception:
            # Ignore permission or transient errors and continue searching
            continue

    raise FileNotFoundError(
        f"No Git repository root found starting from '{start_dir}'. Ensure you're "
        f"running inside a cloned repository with a '.git' directory."
    )


def prepare_profiling_duckdb_sql_file(sql_file: Path) -> Path:
    """
    Prepare the SQL file by adding profiling configuration:
    1. Add PRAGMA enable_profiling='json' at the beginning if not present
    2. Add SET profiling_output before each SQL query statement
    
    Creates a temporary file instead of modifying the original.
    
    Args:
        sql_file: Path to the original SQL file
        
    Returns:
        Path to the temporary profiling SQL file (original_name_profiling_tmp.sql)
    """
    
    # Create temporary file name
    tmp_file = sql_file.parent / f"{sql_file.stem}_profiling_tmp{sql_file.suffix}"

    # Read the original SQL file
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
            new_content_parts.append(f"SET profiling_output='profiling_query_{query_number}.json'")
            new_content_parts.append(statement)

        query_number += 1

    # Reconstruct the SQL file with proper formatting
    new_content = ';\n\n'.join(new_content_parts)
    if new_content and not new_content.endswith(';'):
        new_content += ';'

    # Write to temporary file
    with open(tmp_file, 'w') as f:
        f.write(new_content + '\n')

    print(f"✓ Created temporary SQL file: {tmp_file}")
    return tmp_file
