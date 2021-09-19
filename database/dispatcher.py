from database import cacher
from database import trimmer
from database import utils


def get_index_daily(index_code, days):
    today = utils.get_today()
    cache_name = f'{today}-{index_code}-{days}'
    if cacher.has_cache(cache_name):
        data = cacher.read_cache(cache_name)
    else:
        data = trimmer.get_index_daily(index_code, days)
        cacher.write_cache(data, cache_name)
    return data

