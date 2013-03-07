from __future__ import absolute_import

from urlparse import urlparse
from redis import Redis

from . import CatalogCoordinator
from .redis_lock import Lock

class RedisCatalogCoordinator(CatalogCoordinator):

    def __init__(self, **kwargs):
        super(RedisCatalogCoordinator, self).__init__(**kwargs)
        u = urlparse(self.url)
        self._redis = Redis(host=u.hostname, port=u.port)

    def get_index_lock(self):
        return Lock('index', redis=self._redis)
    
    @classmethod
    def validate_url(cls, url):
        return urlparse(url).scheme in ('redis')

