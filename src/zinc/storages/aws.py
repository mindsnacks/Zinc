import os
from tempfile import TemporaryFile
from copy import copy
from urlparse import urlparse

from boto.s3.key import Key
from boto.s3.connection import S3Connection

from . import StorageBackend


class S3StorageBackend(StorageBackend):

    def __init__(self, url=None, aws_key=None, aws_secret=None,
                 s3connection=None, bucket=None, prefix=None, **kwargs):

        super(S3StorageBackend, self).__init__(**kwargs)

        assert s3connection or (aws_key and aws_secret)
        self._conn = s3connection or S3Connection(aws_key, aws_secret)

        assert bucket or url
        bucket_name = bucket or urlparse(url).netloc

        self._bucket = self._conn.get_bucket(bucket_name)
        self._prefix = prefix

    @classmethod
    def valid_url(cls, url):
        return urlparse(url).scheme in ('s3')

    def bind_to_catalog(self, loc=None, id=None):
        assert id
        cpy = copy(self)
        cpy._prefix = id
        return cpy

    def _get_keyname(self, subpath):
        if self._prefix:
            return os.path.join(self._prefix, subpath)
        else:
            return subpath

    def get(self, subpath):
        t = TemporaryFile()
        t.write(self._bucket.get_key(self._get_keyname(subpath)).read())
        t.seek(0)
        return t

    def get_meta(self, subpath):
        key = self._bucket.lookup(self._get_keyname(subpath))
        if key is None:
            return None
        meta = dict()
        meta['size'] = key.size
        return meta

    def put(self, subpath, fileobj):
        k = Key(self._bucket)
        k.key = self._get_keyname(subpath)
        k.set_contents_from_file(fileobj)
