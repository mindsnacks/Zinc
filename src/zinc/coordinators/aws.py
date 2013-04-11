from urlparse import urlparse
import random
import time
import uuid
import logging
from threading import Timer

import boto.sdb
from boto.exception import SDBResponseError

from . import CatalogCoordinator

log = logging.getLogger(__name__)

LOCK_TOKEN = 'lock_token'
LOCK_TIME = 'lock_time'


class Lock(object):
    def __init__(self, sdb_domain, key, expires=300, timeout=10):

        # Expiration must be 0 (never) or at least 30 seconds and greater than
        # the timeout
        assert expires == 0 or (expires > timeout and expires >= 60)

        self._sdb_domain = sdb_domain
        self._key = key
        self._timeout = timeout
        self._expires = expires
        self._token = str(uuid.uuid1())
        self._refresh = expires / 4

    def _update_lock(self):
        """Attempts to update the lock but incresing the lock_time. Will fail if
        the remote `lock_token` does not match our local `lock_token`."""

        log.debug('Updating lock...')

        attrs = {
            LOCK_TOKEN: self._token,
            LOCK_TIME: time.time()
        }
        self._sdb_domain.put_attributes(
            self._key, attrs,
            expected_value=[LOCK_TOKEN, self._token])

    def __enter__(self):
        timeout = self._timeout
        while timeout >= 0:
            try:
                item = self._sdb_domain.get_item(self._key, consistent_read=True)

                # check expiration
                if item is not None and self._expires != 0:
                    lock_time = item.get(LOCK_TIME)
                    if lock_time is None \
                       or time.time() > float(lock_time) + self._expires:
                        log.info('Clearing expired lock...')
                        self._sdb_domain.delete_attributes(
                            self._key, [LOCK_TOKEN, LOCK_TIME],
                            expected_values=[LOCK_TOKEN, item[LOCK_TOKEN]])

                # try to get the lock
                if item is None or item.get(LOCK_TOKEN) is None:

                    # try to write, expecting token to be UNSET
                    attrs = {
                        LOCK_TOKEN: self._token,
                        LOCK_TIME: time.time()
                    }
                    self._sdb_domain.put_attributes(
                        self._key, attrs,
                        expected_value=[LOCK_TOKEN, False])

                    self._timer = Timer(self._refresh, self._update_lock)
                    self._timer.start()
                    return

                timeout -= 1
                if timeout >= 0:
                    log.debug('Sleeping')
                    time.sleep(1)

            except SDBResponseError, sdberr:
                if sdberr.status == 409:
                    pass  # we will retry
                else:
                    raise sdberr

    def __exit__(self, exc_type, exc_value, traceback):
        self._timer.cancel()
        item = self._sdb_domain.get_item(self._key, consistent_read=True)
        if item is not None and item[LOCK_TOKEN] == self._token:
            self._sdb_domain.delete_attributes(
                self._key, [LOCK_TOKEN, LOCK_TIME],
                expected_values=[LOCK_TOKEN, self._token])
        else:
            raise LockException("Failed to acquire lock within timeout.")


################################################################################


class LockException(Exception):
    pass

################################################################################


class SimpleDBCatalogCoordinator(CatalogCoordinator):

    def __init__(self, url=None, aws_key=None, aws_secret=None,
                 sdb_connection=None, **kwargs):

        super(SimpleDBCatalogCoordinator, self).__init__(**kwargs)

        assert sdb_connection or (aws_key and aws_secret)

        urlcomps = urlparse(url)
        sdb_region = urlcomps.netloc
        sdb_domain = urlcomps.path[1:]  # strip leading /
        if sdb_domain == '':
            sdb_domain = 'zinc'

        self._conn = boto.sdb.connect_to_region(sdb_region,
                                                aws_access_key_id=aws_key,
                                                aws_secret_access_key=aws_secret)
        self._domain = self._conn.create_domain(sdb_domain)

    def get_index_lock(self, domain=None):
        assert domain
        return Lock(self._domain, domain)

    @classmethod
    def valid_url(cls, url):
        urlcomps = urlparse(url)
        return urlcomps.scheme == 'sdb' \
                and urlcomps.netloc in [r.name for r in boto.sdb.regions()]
