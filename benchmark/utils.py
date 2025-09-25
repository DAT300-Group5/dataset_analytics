import os

def load_query_from_file(file_path):
    """
    Helper function to load content from a file.
    
    Args:
        file_path (str): Path to the file to be read
        
    Returns:
        str: File content as string
        
    Raises:
        ValueError: If file_path is empty or None
        FileNotFoundError: If file doesn't exist
        IOError: If file cannot be read
    """
    if not file_path or not file_path.strip():
        raise ValueError("File path cannot be empty or None")
    
    file_path_stripped = file_path.strip()
    
    # Check if file exists
    if not os.path.exists(file_path_stripped):
        raise FileNotFoundError(f"File not found: {file_path_stripped}")
    
    # Check if it's actually a file (not a directory)
    if not os.path.isfile(file_path_stripped):
        raise ValueError(f"Path is not a file: {file_path_stripped}")
    
    try:
        with open(file_path_stripped, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except IOError as e:
        raise IOError(f"Error reading file {file_path_stripped}: {e}")
