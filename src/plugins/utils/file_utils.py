import pathlib


def mkdirs_if_not_exists(path: str) -> None:
    """Create directories if they do not exist.

    Args:
        path (str): The directory path to create.
    """
    directory_path = pathlib.Path(path)

    directory_path.mkdir(exist_ok=True, parents=True)
