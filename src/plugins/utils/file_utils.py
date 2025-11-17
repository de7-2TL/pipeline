import pathlib


def mkdirs_if_not_exists(path: str) -> None:
    """Create directories if they do not exist.

    Args:
        path (str): The directory path to create.
    """
    directory_path = pathlib.Path(path)

    directory_path.mkdir(exist_ok=True, parents=True)

def get_absolute_path(relative_path: str) -> str:
    """
    주어진 상대 경로를 절대 경로로 변환합니다.
    """
    path_obj = pathlib.Path(relative_path)

    absolute_path = path_obj.resolve()

    # 3. 문자열로 반환
    return str(absolute_path)