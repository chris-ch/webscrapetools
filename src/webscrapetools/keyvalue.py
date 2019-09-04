import hashlib
import itertools
import logging
import threading
from typing import Tuple, Iterable, List, MutableSequence

from webscrapetools import osaccess
from datetime import datetime, timedelta


__rebalancing = threading.Condition()
__CACHE_INDEX_NAME = 'index'
__CACHE_FILE_PATH = None
__EXPIRY_DAYS = None
__MAX_NODE_FILES = 0x100
__REBALANCING_LIMIT = 0x200


def _get_cache_file_path():
    global __CACHE_FILE_PATH
    return __CACHE_FILE_PATH


def _get_expiry_days() -> int:
    global __EXPIRY_DAYS
    return __EXPIRY_DAYS


def _get_max_node_files() -> int:
    global __MAX_NODE_FILES
    return __MAX_NODE_FILES


def set_store_path(store_path, max_node_files=None, rebalancing_limit=None, expiry_days=10):
    """
    Required for enabling caching.

    :param store_path:
    :param max_node_files:
    :param rebalancing_limit:
    :param expiry_days:
    :return:
    """
    global __CACHE_FILE_PATH
    global __MAX_NODE_FILES
    global __REBALANCING_LIMIT
    global __EXPIRY_DAYS

    __EXPIRY_DAYS = expiry_days

    if max_node_files is not None:
        __MAX_NODE_FILES = max_node_files

    if rebalancing_limit is not None:
        __REBALANCING_LIMIT = rebalancing_limit

    __CACHE_FILE_PATH = osaccess.create_path_if_not_exists(store_path)
    logging.debug('setting cache path: %s', __CACHE_FILE_PATH)
    invalidate_expired_entries()


def invalidate_expired_entries(as_of_date: datetime=None) -> None:
    """
    :param as_of_date: fake current date (for dev only)
    :return:
    """
    index_name = _fileindex_name()

    if not osaccess.exists_path(index_name):
        return

    if as_of_date is None:
        as_of_date = datetime.today()

    expiry_date = as_of_date - timedelta(days=_get_expiry_days())
    expired_keys = list()

    def gather_expired_keys(line):
        yyyymmdd, key_md5, key_commas = line.strip().split(' ')
        key = key_commas[1:-1]
        key_date = datetime.strptime(yyyymmdd, '%Y%m%d')
        if expiry_date > key_date:
            logging.debug('expired entry for key "%s" (%s)', key_md5[:-1], key)
            expired_keys.append(key)

    osaccess.process_file_by_line(index_name, line_processor=gather_expired_keys)
    _remove_from_cache_multiple(expired_keys)


def is_cache_used() -> bool:
    return _get_cache_file_path() is not None


def _generator_count(a_generator: Iterable) -> int:
    return sum(1 for _ in a_generator)


def _divide_node(path: str, nodes_path: MutableSequence[str]) -> Tuple[str, str]:
    level = len(nodes_path)
    new_node_sup_init = 'FF' * 20
    new_node_inf_init = '7F' + 'FF' * 19
    if level > 0:
        new_node_sup = nodes_path[-1]
        new_node_diff = (int(new_node_sup_init, 16) - int(new_node_inf_init, 16)) >> level
        new_node_inf = '%0.40X' % (int(new_node_sup, 16) - new_node_diff)

    else:
        new_node_sup = new_node_sup_init
        new_node_inf = new_node_inf_init

    new_path_1 = osaccess.create_new_filepath(path, nodes_path, new_node_inf.lower())
    new_path_2 = osaccess.create_new_filepath(path, nodes_path, new_node_sup.lower())
    return new_path_1, new_path_2


def rebalance_cache_tree(path: str, nodes_path: List[str]=None):
    if not nodes_path:
        nodes_path = list()

    current_path = osaccess.merge_directory_paths([path], nodes_path)
    files_node = (node for node in osaccess.gen_files_under(current_path) if node != __CACHE_INDEX_NAME)
    rebalancing_required = _generator_count(itertools.islice(files_node, _get_max_node_files() + 1)) > _get_max_node_files()
    if rebalancing_required:
        new_path_1, new_path_2 = _divide_node(path, nodes_path)
        logging.info('rebalancing required, creating nodes: %s and %s', new_path_1, new_path_2)
        with __rebalancing:
            logging.info('lock acquired: rebalancing started')
            osaccess.create_path_if_not_exists(new_path_1)
            osaccess.create_path_if_not_exists(new_path_2)

            for filename in (node for node in osaccess.gen_files_under(current_path) if node != __CACHE_INDEX_NAME):
                file_path = osaccess.build_file_path(current_path, filename)
                if file_path <= new_path_1:
                    logging.debug('moving %s to %s', filename, new_path_1)
                    osaccess.rename_path(file_path, osaccess.build_file_path(new_path_1, filename))

                else:
                    logging.debug('moving %s to %s', filename, new_path_2)
                    osaccess.rename_path(file_path, osaccess.build_file_path(new_path_2, filename))

        logging.info('lock released: rebalancing completed')

    for directory in osaccess.gen_directories_under(current_path):
        rebalance_cache_tree(path, nodes_path + [directory])


def find_node(digest: str, path=None):
    if not path:
        path = _get_cache_file_path()

    directories = osaccess.gen_directories_under(path)

    if not directories:
        return path

    else:
        target_directory = None
        for directory_name in directories:
            if digest <= directory_name:
                target_directory = directory_name
                break

        if not target_directory:
            raise Exception('Inconsistent cache tree: expected directory "%s" not found', target_directory)

        return find_node(digest, path=osaccess.build_directory_path(path, target_directory))


def get_store_id(key: object) -> str:
    """

    :param key: text uniquely identifying the associated content (typically a full url)
    :return: unique path based on hashed version of the input key
    """
    key = repr(key)
    hash_md5 = hashlib.md5()
    hash_md5.update(key.encode('utf-8'))
    digest = hash_md5.hexdigest()
    target_node = find_node(digest)
    return osaccess.build_file_path(target_node, digest)


def has_store_key(key):
    """
    Checks if specified cache key (typically a full url) corresponds to an entry in the cache.

    :param key:
    :return:
    """
    return osaccess.exists_path(get_store_id(key))

def _fileindex_name():
    return osaccess.build_file_path(_get_cache_file_path(), __CACHE_INDEX_NAME)


def _add_to_cache(key, value):
    __rebalancing.acquire()
    try:
        logging.debug('adding to cache: %s', key)
        filename = get_store_id(key)
        filename_digest = osaccess.get_file_from_filepath(filename)
        index_name = _fileindex_name()
        today = datetime.today().strftime('%Y%m%d')
        osaccess.save_content(filename, value)
        index_entry = '%s %s: "%s"\n' % (today, filename_digest, key)
        osaccess.append_content(index_name, index_entry)

    finally:
        __rebalancing.notify_all()
        __rebalancing.release()

    if osaccess.file_size(index_name) % __REBALANCING_LIMIT == 0:
        logging.debug('rebalancing cache')
        rebalance_cache_tree(_get_cache_file_path())


def _get_from_cache(key):
    __rebalancing.acquire()
    try:
        logging.debug('reading from cache: %s', key)
        content = osaccess.load_file_content(get_store_id(key), encoding='utf-8')
    finally:
        __rebalancing.notify_all()
        __rebalancing.release()

    return content


def _remove_from_cache_multiple(keys):
    __rebalancing.acquire()
    try:
        index_name = _fileindex_name()
        lines = osaccess.load_file_lines(index_name)

        for key in keys:
            filename = get_store_id(key)
            filename_digest = osaccess.get_file_from_filepath(filename)
            logging.info('removing key %s from cache' % key)
            osaccess.remove_file(filename)
            lines = [line for line in lines if line.split(' ')[1] != filename_digest + ':']

        osaccess.save_lines(index_name, lines)

    finally:
        __rebalancing.notify_all()
        __rebalancing.release()


def _remove_from_cache(key):
    _remove_from_cache_multiple([key])


def read_cached(read_func, key):
    """
    :param read_func: function getting the data that will be cached
    :param key: key associated to the cache entry
    :return:
    """
    logging.debug('reading for key: %s', key)
    if is_cache_used():
        if not has_store_key(key):
            content = read_func(key)
            _add_to_cache(key, content)

        content = _get_from_cache(key)

    else:
        # straight access
        content = read_func(key)

    return content


def remove_store_key(key):
    if is_cache_used():
        _remove_from_cache(key)


def rebalance_store():
    if is_cache_used():
        rebalance_cache_tree(_get_cache_file_path())


def empty_store():
    """
    Removing cache content.
    :return:
    """
    if is_cache_used():
        for node in osaccess.get_files_under_path(_get_cache_file_path()):
            node_path = osaccess.build_file_path(_get_cache_file_path(), node)
            osaccess.remove_all_under_path(node_path)

        osaccess.remove_file_if_exists(_fileindex_name())

