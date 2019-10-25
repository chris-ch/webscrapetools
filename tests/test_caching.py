import logging
import os
import random
import unittest
from datetime import datetime
from datetime import timedelta

from webscrapetools.keyvalue import invalidate_expired_entries, set_store_path, add_to_store, retrieve_from_store, \
    remove_from_store, list_keys, empty_store
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

            def inner_open_test_random(inner_key: str) -> str:
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

        self.assertEqual(0, len(list(gen_directories_under(test_output_dir))))
        self.assertEqual(0, len(list(gen_files_under(test_output_dir))))

        def read_random_value(key: str) -> str:
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
        self.assertEqual(0, len(list(gen_directories_under(test_output_dir))))
        self.assertEqual(0, len(list(gen_files_under(test_output_dir))))

    def test_cache_entry(self):
        set_cache_path('./output/tests', max_node_files=400, rebalancing_limit=1000)
        empty_cache()
        value = get_cache_filename('my content')
        self.assertEqual(os.path.abspath('output/tests/bc4e44260919ea00a59f7a9dc75e73e3'), value)
        empty_cache()

    def test_cache_example(self):
        set_cache_path('./output/tests', max_node_files=10, rebalancing_limit=100)
        empty_cache()

        def dummy_client():
            return None

        def dummy_call(_, dummy_key):
            return '{:d}'.format(int(dummy_key)) * int(dummy_key), dummy_key

        keys = ('{:05d}'.format(count) for count in range(500))
        for key in keys:
            open_url(key, init_client_func=dummy_client, call_client_func=dummy_call)

        empty_cache()

    def test_store(self):
        set_store_path('./output/tests', max_node_files=10, rebalancing_limit=30)
        empty_store()
        for count in range(100):
            add_to_store(str(count), bytes(str(count), 'utf-8'))

        value = retrieve_from_store('30')
        self.assertEqual(b'30', value)
        remove_from_store('30')
        value = retrieve_from_store('30')
        self.assertIsNone(value)
        with self.assertRaises(Exception) as _:
            retrieve_from_store('30', fail_on_missing=True)

        keys = list_keys()
        self.assertListEqual(sorted(list(filter(lambda x: x != '30', map(str, range(100))))), keys)
        empty_store()

    def test_store_list_keys(self):
        set_store_path('./output/tests', max_node_files=10, rebalancing_limit=30)
        empty_store()
        for count in range(100):
            add_to_store('value ' + str(count), bytes(str(count), 'utf-8'))

        keys = list_keys()
        self.assertListEqual(sorted(['value ' + str(x) for x in range(100)]), keys)
        empty_store()

    def test_store_duplicate_keys(self):
        set_store_path('./output/tests', max_node_files=10, rebalancing_limit=30)
        empty_store()
        for count in range(5):
            add_to_store(str(count), bytes(str(count), 'utf-8'))

        existing_keys = list_keys()
        self.assertEqual(len(existing_keys), 5)

        for count in range(5):
            add_to_store(str(count), bytes(str(count + 100), 'utf-8'))

        existing_keys = list_keys()
        self.assertEquals(len(existing_keys), 5)

        value = retrieve_from_store('3')
        self.assertEqual(b'103', value)
        remove_from_store('3')
        value = retrieve_from_store('3')
        self.assertIsNone(value)
        with self.assertRaises(Exception) as _:
            retrieve_from_store('3', fail_on_missing=True)

        keys = list_keys()
        self.assertListEqual(sorted(list(filter(lambda x: x != '3', map(str, range(5))))), keys)
        empty_store()

    def tearDown(self):
        empty_cache()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
