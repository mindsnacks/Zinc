import os

from . import StorageBackend

class S3StorageBackend(StorageBackend):

    def __init__(self, s3connection=None, bucket=None, prefix=None, **kwargs):
        assert s3connection
        assert bucket
        super(S3StorageBackend, self).__init__(**kwargs)
        self._conn = s3connection
        self._bucket = self._conn.get_bucket(bucket)
        self._prefix = prefix

    def get(self, subpath):
        if self._prefix:
            key = os.path.join(self._prefix, subpath)
        else:
            key = subpath
        return self._bucket.get_key(key)

