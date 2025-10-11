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

if __name__ == "__main__":
    clean_path("/Users/xiejiangzhao/PycharmProject/dataset_analytics/benchmark/test/results")