import os
from shutil import rmtree
import logging
from typing import Iterable, List, Callable


class Path(object):
    pass


class FilePath(Path):
    pass


class DirectoryPath(Path):
    pass


class _OSFilePath(FilePath):

    def __init__(self, path: str):
        FilePath.__init__(self)
        self._path = path


class _OSDirectoryPath(DirectoryPath):

    def __init__(self, path: str):
        DirectoryPath.__init__(self)
        self._path = path


def create_path_if_not_exists(path: str) -> str:
    path_full = os.path.abspath(path)
    if not os.path.exists(path_full):
        os.makedirs(path_full)

    return path_full


def rename_path(old_path, new_path):
    os.rename(old_path, new_path)


def create_new_filepath(path_prefix, path, filename):
    return os.path.abspath(os.path.sep.join([path_prefix] + path + [filename]))


def exists_path(filename: str) -> bool:
    return os.path.exists(filename)


def build_file_path(path: str, filename) -> str:
    return os.path.sep.join([path, filename])


def build_directory_path(path, filename):
    return os.path.sep.join([path, filename])


def merge_directory_paths(path_first: List[str], path_next: List[str]):
    return os.path.sep.join(path_first + path_next)


def file_size(filename):
    count = -1
    with open(filename) as file_lines:
        for count, line in enumerate(file_lines):
            pass

    return count + 1


def get_file_from_filepath(path):
    return path.split(os.path.sep)[-1]


def remove_file_if_exists(filename):
    if os.path.exists(filename):
        remove_file(filename)


def remove_file(filename):
    try:
        if os.path.isfile(filename):
            os.remove(filename)

    except FileNotFoundError:
        logging.warning('broken reference to file %s', filename)


def get_files_under_path(path: str) -> Iterable[str]:
    return os.listdir(path)


def remove_all_under_path(path):
    if os.path.isfile(path):
        os.remove(path)

    else:
        rmtree(path, ignore_errors=True)


def gen_directories_under(path):
    return sorted(node for node in os.listdir(path) if os.path.isdir(os.path.join(path, node)))


def gen_files_under(path):
    return sorted(node for node in os.listdir(path) if os.path.isfile(os.path.join(path, node)))


def load_file_content(filepath: str) -> bytes:
    with open(filepath, 'rb') as cache_content:
        content = cache_content.read()

    return content


def load_file_lines(filepath, encoding='utf-8'):
    with open(filepath, 'r', encoding=encoding) as myfile:
        lines = myfile.readlines()

    return lines


def process_file_by_line(filename: str, line_processor: Callable[[str], None]) -> None:
    with open(filename, 'r') as index_file:
        lines = index_file.readlines()
        for line in lines:
            if len(line.strip()) == 0:
                continue

            line_processor(line)


def save_lines(filepath: str, lines: Iterable[str]) -> None:
    with open(filepath, 'w') as myfile:
        myfile.writelines(lines)


def save_content(filename: str, content: bytes) -> None:
    with open(filename, 'wb') as myfile:
        myfile.write(content)


def append_content(filepath: str, content: bytes):
    if not exists_path(filepath):
        with open(filepath, 'wb') as myfile:
            myfile.write(content)

    else:
        with open(filepath, 'ab') as myfile:
            myfile.write(content)
