import os

def create_directory(dir_path: str):
    """
    Creates a directory, if is does not exists.

    Args:
        dir_path (str): directory to be created.
    """
    
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)