import os
from urlparse import urlparse
from atomicfile import AtomicFile
from copy import copy

import zinc.utils as utils
from . import StorageBackend


class FilesystemStorageBackend(StorageBackend):

    def __init__(self, url=None, **kwargs):
        super(FilesystemStorageBackend, self).__init__(**kwargs)
        assert url is not None
        self._url = url

    @classmethod
    def valid_url(cls, url):
        return urlparse(url).scheme in ('file')

    def bind_to_catalog(self, id=None):
        assert id
        cpy = copy(self)
        urlcomps = urlparse(self._url)
        new_path = os.path.join(urlcomps.path, id)
        cpy._url = 'file://%s' % (new_path)
        return cpy

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

    def put(self, subpath, fileobj, **kwargs):
        abs_path = self._abs_path(subpath)
        utils.makedirs(os.path.dirname(abs_path))
        with AtomicFile(abs_path, 'w') as f:
            f.write(fileobj.read())

    def list(self, prefix=None):
        if prefix is not None:
            dir = self._abs_path(prefix)
        else:
            dir = self._root_abs_path()

        contents = []
        for path, dirs, files in os.walk(dir):
            for fn in files:
                abs_path = os.path.join(path, fn)
                rel_path = abs_path[len(dir) + 1:]  # get path relative to dir
                contents.append(rel_path)

        return contents

    def delete(self, subpath):
        path = self._abs_path(subpath)
        os.remove(path)
