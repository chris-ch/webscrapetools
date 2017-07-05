A basic but threadsafe caching system...

Example usage:

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

