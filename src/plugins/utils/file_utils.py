import pathlib


def mkdirs_if_not_exists(path: str) -> None:
    """Create directories if they do not exist.

    Args:
        path (str): The directory path to create.
    """
    directory_path = pathlib.Path(path)

    directory_path.mkdir(exist_ok=True, parents=True)

def get_path(file_path: str, parents: int = 2) -> str:
    from pathlib import Path


    project_root = Path(__file__).resolve().parents[parents]

    return str((f"{project_root}/{file_path}"))