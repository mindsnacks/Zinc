import os
from urlparse import urlparse
from atomicfile import AtomicFile

from . import StorageBackend
from zinc.utils import *


class FilesystemStorageBackend(StorageBackend):

    def __init__(self, url=None, **kwargs):
        super(FilesystemStorageBackend, self).__init__(**kwargs)
        assert url is not None
        self._url = url

    @classmethod
    def valid_url(cls, url):
        return urlparse(url).scheme in ('file')

    @property
    def url(self):
        return self._url

    def _root_abs_path(self):
        return urlparse(self.url).path

    def _abs_path(self, subpath):
        return os.path.join(self._root_abs_path(), subpath)

    def get(self, subpath):
        abs_path = self._abs_path(subpath)
        f = open(abs_path, 'r')
        return f

    def get_meta(self, subpath):
        abs_path = self._abs_path(subpath)
        if not os.path.exists(abs_path):
            return None
        meta = dict()
        meta['size'] = os.path.getsize(abs_path)
        return meta

    def put(self, subpath, fileobj):
        abs_path = self._abs_path(subpath)
        makedirs(os.path.dirname(abs_path))
        with AtomicFile(abs_path, 'w') as f:
            f.write(fileobj.read())
