import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from . import StorageBackend

class S3StorageBackend(StorageBackend):

    def __init__(self, bucket=None, key=None, secret=None, prefix=None, **kwargs):
        assert bucket
        assert key
        assert secret
        super(S3StorageBackend, self).__init__(**kwargs)
        self._conn = S3Connection(key, secret)
        self._bucket = self._conn.get_bucket(bucket)
        self._prefix = prefix

    def get(self, subpath):
        if self._prefix:
            key = os.path.join(self._prefix, subpath)
        else:
            key = subpath
        return self._bucket.get_key(key)

