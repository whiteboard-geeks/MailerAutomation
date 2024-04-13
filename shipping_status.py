import requests
import requests_cache


requests_cache.install_cache('cache')
cache = requests_cache.get_cache()
