from urlparse import urlparse
import time
import uuid
import logging
from threading import Timer

import boto.sdb
from boto.exception import SDBResponseError

from . import CatalogCoordinator, LockException

log = logging.getLogger(__name__)

LOCK_TOKEN = 'lock_token'
LOCK_EXPIRES = 'lock_expiry'


class Lock(object):
    def __init__(self, sdb_domain, key, expires=None, timeout=None):

        self._timeout = timeout or 10
        self._expires = expires or 300

        # Expiration must be 0 (never) or at least 30 seconds and greater than
        # the timeout
        assert self._expires == 0 or (self._expires > self._timeout and
                                      self._expires >= 60)

        self._sdb_domain = sdb_domain
        self._key = key
        self._token = str(uuid.uuid1())
        self._refresh = self._expires / 4
        self._timer = None
        self._is_locked = False

    def _update_lock(self):
        """Attempts to update the lock by increasing the lock expiration. Will
        fail if the remote `lock_token` does not match our local
        `lock_token`."""

        attrs = self._get_lock_attrs()
        log.debug('Refreshing lock... %s' % (attrs))

        self._sdb_domain.put_attributes(
            self._key, attrs,
            expected_value=[LOCK_TOKEN, self._token])

    def _schedule_timer(self):
        self._timer = Timer(self._refresh, self._timer_fired)
        self._timer.start()

    def _timer_fired(self):
        self._update_lock()
        self._schedule_timer()

    def _get_lock_attrs(self):
        return {
            LOCK_TOKEN: self._token,
            LOCK_EXPIRES: time.time() + self._expires
        }

    def is_locked(self):
        return self._is_locked

    def acquire(self):

        if self.is_locked():
            return

        timeout = self._timeout
        while timeout >= 0:
            try:
                item = self._sdb_domain.get_item(self._key, consistent_read=True)

                # check expiration
                if item is not None and self._expires != 0:
                    lock_expires = item.get(LOCK_EXPIRES)
                    if lock_expires is None \
                       or time.time() > float(lock_expires):
                        log.info('Clearing expired lock...')
                        self._sdb_domain.delete_attributes(
                            self._key, [LOCK_TOKEN, LOCK_EXPIRES],
                            expected_values=[LOCK_TOKEN, item[LOCK_TOKEN]])

                # try to get the lock
                if item is None or item.get(LOCK_TOKEN) is None:

                    # try to write, expecting token to be UNSET
                    self._sdb_domain.put_attributes(
                        self._key, self._get_lock_attrs(),
                        expected_value=[LOCK_TOKEN, False])

                    self._is_locked = True
                    self._schedule_timer()
                    return

                timeout -= 1
                if timeout >= 0:
                    log.debug('Sleeping')
                    time.sleep(1)
                else:
                    raise LockException("Failed to acquire lock within timeout.")

            except SDBResponseError, sdberr:
                if sdberr.status == 409:
                    pass  # we will retry
                else:
                    raise sdberr

    def release(self):
        if self._timer is not None:
            self._timer.cancel()
        item = self._sdb_domain.get_item(self._key, consistent_read=True)
        if item is not None and item[LOCK_TOKEN] == self._token:
            self._sdb_domain.delete_attributes(
                self._key, [LOCK_TOKEN, LOCK_EXPIRES],
                expected_values=[LOCK_TOKEN, self._token])
        self._is_locked = False

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


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

    def get_index_lock(self, domain=None, timeout=None, **kwargs):
        assert domain
        return Lock(self._domain, domain, timeout=timeout)

    @classmethod
    def valid_url(cls, url):
        urlcomps = urlparse(url)
        return urlcomps.scheme == 'sdb' \
                and urlcomps.netloc in [r.name for r in boto.sdb.regions()]
