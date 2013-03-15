from __future__ import absolute_import

import time, random
from urlparse import urlparse
from redis import Redis

from . import CatalogCoordinator

################################################################################

# a slightly modified version of retools lock which depends only on redis' expire.
# (retools depends on client OS time being sync'd)

class Lock(object):
    def __init__(self, key, expires=30, timeout=10, redis=None):
        """
        Distributed locking using Redis SETNX and GETSET.

        Usage::

            with Lock('my_lock'):
                print "Critical section"

        :param  expires:    We consider any existing lock older than
                            ``expires`` seconds to be invalid in order to
                            detect crashed clients. This value must be higher
                            than it takes the critical section to execute.
        :param  timeout:    If another client has already obtained the lock,
                            sleep for a maximum of ``timeout`` seconds before
                            giving up. A value of 0 means we never wait.
        :param  redis:      The redis instance to use if the default global
                            redis connection is not desired.

        """
        self.key = key
        self.timeout = timeout
        self.expires = expires
        if not redis:
            redis = Redis()
        self.redis = redis
        self.token = str(time.time() * random.random())

    def __enter__(self):
        redis = self.redis
        timeout = self.timeout
        while timeout >= 0:
            if redis.setnx(self.key, self.token):
                # We gained the lock; enter critical section
                redis.expire(self.key, int(self.expires))
                return

            timeout -= 1
            if timeout >= 0:
                time.sleep(1)
        raise LockException("Timeout while waiting for lock.")

    def __exit__(self, exc_type, exc_value, traceback):
        # Only delete the key if it's our token
        current_value = self.redis.get(self.key)

        if current_value == self.token:
            self.redis.delete(self.key)
        else:
            raise LockException("Lock expired before exit.")

################################################################################


class LockException(Exception):
    pass

################################################################################


class RedisCatalogCoordinator(CatalogCoordinator):

    def __init__(self, redis=None, redis_password=None, **kwargs):
        super(RedisCatalogCoordinator, self).__init__(**kwargs)
        assert self.url or redis
        if redis is not None:
            self._redis = redis
        else:
            u = urlparse(self.url)
            self._redis = Redis(host=u.hostname, port=u.port, password=redis_password)

    def get_index_lock(self, prefix=None):
        name = 'index'
        if prefix:
            name = '%s.%s' % (prefix, name)
        return Lock(name, redis=self._redis)

    @classmethod
    def valid_url(cls, url):
        return urlparse(url).scheme in ('redis')
