import os

from boto.s3.key import Key

from . import StorageBackend

class S3StorageBackend(StorageBackend):

    def __init__(self, s3connection=None, bucket=None, prefix=None, **kwargs):
        assert s3connection
        assert bucket
        super(S3StorageBackend, self).__init__(**kwargs)
        self._conn = s3connection
        self._bucket = self._conn.get_bucket(bucket)
        self._prefix = prefix

    def _get_keyname(self, subpath):
        if self._prefix:
            return os.path.join(self._prefix, subpath)
        else:
            return subpath

    def get(self, subpath):
        return self._bucket.get_key(
                self._get_keyname(subpath))

    def put(self, subpath, fileobj):
        k = Key(self._bucket)
        k.key = self._get_keyname(subpath)
        k.set_contents_from_file(fileobj)

