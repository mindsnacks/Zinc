from urllib.parse import urlparse
import time
import uuid
import logging
from threading import Timer

import boto3
import botocore.exceptions

from . import CatalogCoordinator, LockException

log = logging.getLogger(__name__)

LOCK_TOKEN = 'lock_token'
LOCK_EXPIRES = 'lock_expiry'


class Lock(object):
    def __init__(self, sdb_client, sdb_domain_name, key, expires=None, timeout=None):

        self._timeout = timeout or 10
        self._expires = expires or 300

        # Expiration must be 0 (never) or at least 30 seconds and greater than
        # the timeout
        assert self._expires == 0 or (self._expires > self._timeout and self._expires >= 60)

        self._sdb_client = sdb_client
        self._sdb_domain_name = sdb_domain_name
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

        self._sdb_client.put_attributes(
            DomainName=self._sdb_domain_name,
            ItemName=self._key,
            Attributes=[
                {
                    'Name': LOCK_EXPIRES,
                    'Value': f"{time.time() + self._expires}",
                    'Replace': True
                },
            ],
            Expected={
                'Name': LOCK_TOKEN,
                'Value': self._token,
                'Exists': True
            }
        )

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
                response = self._sdb_client.get_attributes(
                    DomainName=self._sdb_domain_name,
                    ItemName=self._key,
                    AttributeNames=[LOCK_TOKEN, LOCK_EXPIRES],
                    ConsistentRead=True
                )
                lock_expires = None
                lock_token = None
                for attribute in response['Attributes']:
                    if attribute['Name'] == LOCK_EXPIRES:
                        lock_expires = attribute['Value']
                    elif attribute['Name'] == LOCK_TOKEN:
                        lock_token = attribute['Value']
                if self._expires != 0:
                    if lock_expires is None or time.time() > float(lock_expires):
                        log.info('Clearing expired lock...')
                        self._sdb_client.delete_attributes(
                            DomainName=self._sdb_domain_name,
                            ItemName=self._key,
                            Attributes=[
                                {
                                    'Name': LOCK_EXPIRES,
                                    'Value': lock_expires or '',
                                 },
                                {
                                    'Name': LOCK_TOKEN,
                                    'Value': lock_token or '',
                                },
                            ],
                            Expected={
                                'Name': LOCK_TOKEN,
                                'Value': lock_token,
                                'Exists': True
                            }
                        )
                if lock_token is None:
                    self._sdb_client.put_attributes(
                        DomainName=self._sdb_domain_name,
                        ItemName=self._key,
                        Attributes=[
                            {
                                'Name': LOCK_TOKEN,
                                'Value': self._token
                            },
                            {
                                'Nane': LOCK_EXPIRES,
                                'Value': f"{time.time() + self._expires}"
                            },
                        ],
                        Expected={
                            'Name': LOCK_TOKEN,
                            'Exists': False
                        }
                    )
                    self._is_locked = True
                    self._schedule_timer()
                    return

                timeout -= 1
                if timeout >= 0:
                    log.debug('Sleeping')
                    time.sleep(1)
                else:
                    raise LockException("Failed to acquire lock within timeout.")

            except botocore.exceptions.ClientError as error:
                error_code = error.response['Error']['Code']
                if error_code == '409':
                    pass  # we will retry
                else:
                    raise error

    def release(self):
        if self._timer is not None:
            self._timer.cancel()
        response = self._sdb_client.get_attributes(
            DomainName=self._sdb_domain_name,
            ItemName=self._key,
            AttributeNames=[LOCK_TOKEN],
            ConsistentRead=True
        )
        lock_token = None
        for attribute in response['Attributes']:
            if attribute['Name'] == LOCK_TOKEN:
                lock_token = attribute['Value']
        if lock_token == self._token:
            self._sdb_client.delete_attributes(
                DomainName=self._sdb_domain_name,
                ItemName=self._key,
                Attributes=[
                    {
                        'Name': LOCK_TOKEN,
                        'Value': self._token,
                    },
                    {
                        'Name': LOCK_EXPIRES,
                        'Value': '',
                    },
                ],
                Expected={
                    'Name': LOCK_TOKEN,
                    'Value': self._token,
                    'Exists': True
                }
            )
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

        session = boto3.session.Session(
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=sdb_region,
        )
        client = session.client('sdb')

        client.create_domain(DomainName=sdb_domain)

        self._client = client
        self._domain_name = sdb_domain

    def get_index_lock(self, domain=None, timeout=None, **kwargs):
        assert domain
        return Lock(self._client, self._domain_name, domain, timeout=timeout)

    @classmethod
    def valid_url(cls, url):
        urlcomps = urlparse(url)
        s = boto3.session.Session()
        return urlcomps.scheme == 'sdb' and urlcomps.netloc in s.get_available_regions('sdb')
