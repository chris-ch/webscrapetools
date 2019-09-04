import logging
import os
import random
import unittest
from datetime import datetime
from datetime import timedelta

from webscrapetools.keyvalue import invalidate_expired_entries, set_store_path, add_to_store, retrieve_from_store, \
    remove_from_store
from webscrapetools.osaccess import gen_directories_under, gen_files_under
from webscrapetools.taskpool import TaskPool

from webscrapetools.urlcaching import set_cache_path, read_cached, empty_cache, is_cached, \
    get_cache_filename, open_url


class TestUrlCaching(unittest.TestCase):

    def setUp(self):
        pass

    def test_random_access_multithreaded(self):
        set_cache_path('./output/tests', max_node_files=400, rebalancing_limit=1000)
        empty_cache()
        tasks = TaskPool(30)

        def open_test_random(key):

            def inner_open_test_random(inner_key):
                return 'content for key %s: %s' % (inner_key, random.randint(1, 100000))

            content = read_cached(inner_open_test_random, key)
            return content

        for count in range(10000):
            tasks.add_task(open_test_random, count)

        results = tasks.execute()
        logging.info('results: %s', results)
        empty_cache()

    def test_expiration(self):
        test_output_dir = './output/tests'
        set_cache_path(test_output_dir, max_node_files=400, rebalancing_limit=1000, expiry_days=3)
        empty_cache()

        self.assertEqual(len(list(gen_directories_under(test_output_dir))), 0)
        self.assertEqual(len(list(gen_files_under(test_output_dir))), 0)

        def read_random_value(key):
            return 'content for key %s: %s' % (key, random.randint(1, 100000))

        read_cached(read_random_value, key='abc')
        read_cached(read_random_value, key='def')
        read_cached(read_random_value, key='ghf')

        self.assertTrue(is_cached('abc'))
        self.assertTrue(is_cached('def'))
        self.assertTrue(is_cached('ghf'))

        future_date = datetime.today() + timedelta(days=10)
        invalidate_expired_entries(as_of_date=future_date)
        self.assertFalse(is_cached('abc'))
        self.assertFalse(is_cached('def'))
        self.assertFalse(is_cached('ghf'))

        empty_cache()
        self.assertEqual(len(list(gen_directories_under(test_output_dir))), 0)
        self.assertEqual(len(list(gen_files_under(test_output_dir))), 0)

    def test_cache_entry(self):
        set_cache_path('./output/tests', max_node_files=400, rebalancing_limit=1000)
        empty_cache()
        value = get_cache_filename('my content')
        self.assertEqual(value, os.path.abspath('output/tests/bc4e44260919ea00a59f7a9dc75e73e3'))
        empty_cache()

    def test_cache_example(self):
        set_cache_path('./output/tests', max_node_files=10, rebalancing_limit=100)

        def dummy_client():
            return None

        def dummy_call(_, key):
            return '{:d}'.format(int(key)) * int(key), key

        keys = ('{:05d}'.format(count) for count in range(500))
        for key in keys:
            open_url(key, init_client_func=dummy_client, call_client_func=dummy_call)

        empty_cache()

    def test_store(self):
        set_store_path('./output/tests', max_node_files=10, rebalancing_limit=100)
        for count in range(1000):
            add_to_store(count, str(count))

        value = retrieve_from_store(300)
        self.assertEqual(value, '300')
        remove_from_store(300)
        value = retrieve_from_store(300)
        self.assertIsNone(value)
        with self.assertRaises(Exception) as _:
            retrieve_from_store(300, fail_on_missing=True)

        empty_cache()

    def tearDown(self):
        empty_cache()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
