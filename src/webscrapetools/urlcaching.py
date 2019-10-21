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
from time import sleep
from typing import Callable

import requests

from webscrapetools.keyvalue import set_store_path, empty_store, get_store_id, remove_from_store, \
    has_store_key, is_store_enabled, add_to_store, retrieve_from_store


__all__ = ['open_url', 'set_cache_path', 'empty_cache', 'get_cache_filename', 'invalidate_key', 'is_cached',
           'read_cached', 'set_headers_browser']

__HEADERS_CHROME = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


__web_client = None
__last_request = None

_headers_browser = __HEADERS_CHROME


def set_headers_browser(headers):
    global _headers_browser
    _headers_browser = headers


def _get_headers_browser():
    return _headers_browser


def set_cache_path(cache_file_path, max_node_files=None, rebalancing_limit=None, expiry_days=10):
    set_store_path(cache_file_path, max_node_files, rebalancing_limit, expiry_days)


def invalidate_key(key):
    if is_cached(key):
        remove_from_store(key)


def empty_cache():
    empty_store()


def is_cached(key):
    return has_store_key(key)


def get_cache_filename(key: str) -> str:
    """

    :param key: text uniquely identifying the associated content (typically a full url)
    :return: unique path based on hashed version of the input key
    """
    return get_store_id(key)


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


def read_cached(read_func: Callable[[str], str], key: str) -> str:
    """
    :param read_func: function getting the data that will be cached
    :param key: key associated to the cache entry
    :return:
    """
    logging.debug('reading for key: %s', key)
    if is_store_enabled():
        if not has_store_key(key):
            content = read_func(key)
            add_to_store(key, bytes(content, 'utf-8'))

        content = retrieve_from_store(key).decode('utf-8')

    else:
        # straight access
        content = read_func(key)

    return content


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
            response = __web_client.get(request_url, headers=_get_headers_browser())
            response_text = response.text
            __last_request = response.request

        else:
            response_text, __last_request = call_client_func(__web_client, request_url)

        if rejection_marker is not None and rejection_marker in response_text:
            raise RuntimeError('rejected, failed to load url %s', request_url)

        return response_text

    content = read_cached(inner_open_url, url)
    return content


def reset_client():
    """
    Forces a new client to be used for subsequent calls.

    :return:
    """
    global __web_client
    __web_client = None
