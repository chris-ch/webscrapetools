"""
Helper for webscraping.
Locally caches downloaded pages.
As opposed to requests_cache it should be able to handle multithreading.
The line below enables caching and sets the cached files path:
    >>> set_cache_path('./example-cache')
    >>> first_call_response = open_url('https://www.google.ch/search?q=what+time+is+it')

Subsequent calls for the same URL returns the cached data:
    >>> import time
    >>> time.sleep(60)
    >>> second_call_response = open_url('https://www.google.ch/search?q=what+time+is+it')
    >>> first_call_response == second_call_response
    True

"""
import logging

import itertools
import threading

from datetime import datetime, timedelta
from time import sleep
from typing import Iterable, List, MutableSequence, Tuple

import requests
import hashlib


from webscrapetools.osaccess import create_path_if_not_exists, exists_path, build_file_path, file_size, \
    get_file_from_filepath, remove_file, get_files_under_path, remove_file_if_exists, remove_all_under_path, \
    get_files_under, get_directories_under, build_directory_path, rename_path, create_new_filepath, load_file_content, \
    process_file_by_line, save_lines, save_content, append_content, load_file_lines

__CACHE_FILE_PATH = None
__EXPIRY_DAYS = None
__MAX_NODE_FILES = 0x100
__REBALANCING_LIMIT = 0x200
__HEADERS_CHROME = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


__rebalancing = threading.Condition()
__web_client = None
__last_request = None

__all__ = ['open_url', 'set_cache_path', 'empty_cache', 'get_cache_filename', 'invalidate_key', 'is_cached']


def _get_cache_file_path():
    global __CACHE_FILE_PATH
    return __CACHE_FILE_PATH


def _get_expiry_days() -> int:
    global __EXPIRY_DAYS
    return __EXPIRY_DAYS


def _get_max_node_files() -> int:
    global __MAX_NODE_FILES
    return __MAX_NODE_FILES


def get_web_client():
    """
    Underlying requests session.

    :return:
    """
    return __web_client


def get_last_request():
    """
    Last request.

    :return:
    """
    return __last_request


def set_cache_path(cache_file_path, max_node_files=None, rebalancing_limit=None, expiry_days=10):
    """
    Required for enabling caching.

    :param cache_file_path:
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

    __CACHE_FILE_PATH = create_path_if_not_exists(cache_file_path)
    logging.debug('setting cache path: %s', __CACHE_FILE_PATH)
    invalidate_expired_entries()


def invalidate_expired_entries(as_of_date: datetime=None) -> None:
    """
    :param as_of_date: fake current date (for dev only)
    :return:
    """
    index_name = _fileindex_name()

    if not exists_path(index_name):
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

    process_file_by_line(index_name, line_processor=gather_expired_keys)
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

    new_path_1 = create_new_filepath(path, nodes_path, new_node_inf.lower())
    new_path_2 = create_new_filepath(path, nodes_path, new_node_sup.lower())
    return new_path_1, new_path_2


def rebalance_cache_tree(path, nodes_path=None):
    if not nodes_path:
        nodes_path = list()

    current_path = build_directory_path(path, nodes_path)
    files_node = get_files_under(current_path)
    rebalancing_required = _generator_count(itertools.islice(files_node, _get_max_node_files() + 1)) > _get_max_node_files()
    if rebalancing_required:
        new_path_1, new_path_2 = _divide_node(path, nodes_path)
        logging.info('rebalancing required, creating nodes: %s and %s', new_path_1, new_path_2)
        with __rebalancing:
            logging.info('lock acquired: rebalancing started')
            create_path_if_not_exists(new_path_1)
            create_path_if_not_exists(new_path_2)

            for filename in get_files_under(current_path):
                file_path = build_file_path(current_path, filename)
                if file_path <= new_path_1:
                    logging.debug('moving %s to %s', filename, new_path_1)
                    rename_path(file_path, build_file_path(new_path_1, filename))

                else:
                    logging.debug('moving %s to %s', filename, new_path_2)
                    rename_path(file_path, build_file_path(new_path_2, filename))

        logging.info('lock released: rebalancing completed')

    for directory in get_directories_under(current_path):
        rebalance_cache_tree(path, nodes_path + [directory])


def find_node(digest: str, path=None):
    if not path:
        path = _get_cache_file_path()

    directories = get_directories_under(path)

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

        return find_node(digest, path=build_directory_path(path, target_directory))


def get_cache_filename(key: object) -> str:
    """

    :param key: text uniquely identifying the associated content (typically a full url)
    :return: hashed version of the input key
    """
    key = repr(key)
    hash_md5 = hashlib.md5()
    hash_md5.update(key.encode('utf-8'))
    digest = hash_md5.hexdigest()
    target_node = find_node(digest)
    cache_filename = build_file_path(target_node, digest)
    return cache_filename


def is_cached(key):
    """
    Checks if specified cache key (typically a full url) corresponds to an entry in the cache.

    :param key:
    :return:
    """
    cache_filename = get_cache_filename(key)
    return exists_path(cache_filename)


def _fileindex_name():
    return build_file_path(_get_cache_file_path(), 'index')


def _add_to_cache(key, value):
    __rebalancing.acquire()
    try:
        logging.debug('adding to cache: %s', key)
        filename = get_cache_filename(key)
        filename_digest = get_file_from_filepath(filename)
        index_name = _fileindex_name()
        today = datetime.today().strftime('%Y%m%d')
        save_content(filename, value)
        index_entry = '%s %s: "%s"\n' % (today, filename_digest, key)
        append_content(index_name, index_entry)

    finally:
        __rebalancing.notify_all()
        __rebalancing.release()

    if file_size(index_name) % __REBALANCING_LIMIT == 0:
        logging.debug('rebalancing cache')
        rebalance_cache_tree(_get_cache_file_path())


def _get_from_cache(key):
    __rebalancing.acquire()
    try:
        logging.debug('reading from cache: %s', key)
        content = load_file_content(get_cache_filename(key), encoding='utf-8')
    finally:
        __rebalancing.notify_all()
        __rebalancing.release()

    return content


def _remove_from_cache_multiple(keys):
    __rebalancing.acquire()
    try:
        index_name = _fileindex_name()
        lines = load_file_lines(index_name)

        for key in keys:
            filename = get_cache_filename(key)
            filename_digest = get_file_from_filepath(filename)
            logging.info('removing key %s from cache' % key)
            remove_file(filename)
            lines = [line for line in lines if line.split(' ')[1] != filename_digest + ':']

        save_lines(index_name, lines)

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
        if not is_cached(key):
            content = read_func(key)
            _add_to_cache(key, content)

        content = _get_from_cache(key)

    else:
        # straight access
        content = read_func(key)

    return content


def invalidate_key(key):
    if is_cache_used():
        _remove_from_cache(key)


def rebalance_cache():
    if is_cache_used():
        rebalance_cache_tree(_get_cache_file_path())


def empty_cache():
    """
    Removing cache content.
    :return:
    """
    if is_cache_used():
        for node in get_files_under_path(_get_cache_file_path()):
            node_path = build_file_path(_get_cache_file_path(), node)
            remove_all_under_path(node_path)

        remove_file_if_exists(_fileindex_name())


def open_url(url, rejection_marker=None, throttle=None, init_client_func=None, call_client_func=None):
    """
    Opens specified url. Caching is used if initialized with set_cache_path().
    :param url: target url
    :param rejection_marker: raises error if response contains specified marker
    :param throttle: waiting period before sending request
    :param init_client_func(): function that returns a web client instance
    :param call_client_func(web_client): function that handles a call through the web client and returns (response content, last request)
    :return: remote response as text
    """
    global __web_client

    if __web_client is None:
        if init_client_func is None:
            __web_client = requests.Session()

        else:
            __web_client = init_client_func()

    def inner_open_url(request_url):
        global __last_request
        if throttle:
            sleep(throttle)

        if call_client_func is None:
            response = __web_client.get(request_url, headers=__HEADERS_CHROME)
            response_text = response.text
            __last_request = response.request

        else:
            response_text, __last_request = call_client_func(__web_client, request_url)

        if rejection_marker is not None and rejection_marker in response_text:
            raise RuntimeError('rejected, failed to load url %s', request_url)

        return response_text

    content = read_cached(inner_open_url, url)
    return content
