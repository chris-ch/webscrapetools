A basic but fast, persistent and threadsafe caching system
================================================================

This package lets you efficiently retrieve pages from the Internet by caching request's results.

Basic commands
-----------------

Importing required modules first:

`from webscrapetools import urlcaching`
 
Initializing the cache:

`urlcaching.set_cache_path('.wst_cache')`
    
The option _expiry_days_ sets the cache expiry period, default is 10 days.
    
This is a required step: otherwise responses to url calls will simply not be cached.
Cache data are stored in the specified folder, so that re-using the same string makes the cache persistent. This creates
the folder on the fly if it does not exist.
The following command cleans up the cache, making sure we start with no prior data:

`urlcaching.empty_cache()`
    
Opening an url with the following command stores the repsonse content behind the scene, so that subsequent calls will
not hit the network.

`urlcaching.open_url('http://www.google.com')`
    
    
Full example
-----------------

.. code-block:: python

    from webscrapetools import urlcaching
    import time

    # Initializing the cache
    urlcaching.set_cache_path('.wst_cache')

    # Making sure we start from scratch
    urlcaching.empty_cache()

    # Demo with 5 identical calls... only the first one is delayed, all others are hitting the cache
    count_calls = 1
    while count_calls <= 5:
        start_time = time.time()
        urlcaching.open_url('http://deelay.me/5000/http://www.google.com')
        duration = time.time() - start_time
        print('duration for call {}: {:0.2f}'.format(count_calls, duration))
        count_calls += 1

    # Cleaning up
    urlcaching.empty_cache()

The code above outputs the following:

    duration for call 1: 6.74
    duration for call 2: 0.00
    duration for call 3: 0.00
    duration for call 4: 0.00
    duration for call 5: 0.00

Example plugging in a custom client
--------------------------------------

The framework lets you customize the way you access the web. It is therefore possible to drive a browser 
via Selenium for example.

.. code-block:: python

    from webscrapetools import urlcaching
    urlcaching.set_cache_path('./output/tests', max_node_files=10, rebalancing_limit=100)

    def dummy_client():
        return None

    def dummy_call(_, key):
        return '{:d}'.format(int(key)) * int(key), key

    # simulating calls using the dummy client
    keys = ('{:05d}'.format(count) for count in range(500))
    for key in keys:
        urlcaching.open_url(key, init_client_func=dummy_client, call_client_func=dummy_call)

    urlcaching.empty_cache()
