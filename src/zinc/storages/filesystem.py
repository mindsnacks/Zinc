import os
from urlparse import urlparse

from zinc.catalog import StorageBackend
from zinc.utils import *

class FilesystemStorageBackend(StorageBackend):

    def _root_abs_path(self):
        return urlparse(self.url).path

    def _abs_path(self, subpath):
        return os.path.join(self._root_abs_path(), subpath)

    def get(self, subpath):
        abs_path = self._abs_path(subpath)
        with open(abs_path, 'r') as f:
            d = f.read()
        return d

    def put(self, subpath, bytes):
        abs_path = self._abs_path(subpath)
        makedirs(os.path.dirname(abs_path))
        with open(abs_path, 'w') as f:
            f.write(bytes)


