import os
from urlparse import urlparse

from lockfile import FileLock

from . import CatalogCoordinator


class FilesystemCatalogCoordinator(CatalogCoordinator):

    def _index_lock_path(self):
        return os.path.join(self.path, '.index')

    def get_index_lock(self, **kwargs):
        return FileLock(self._index_lock_path())

    @property
    def path(self):
        return urlparse(self.url).path

    @classmethod
    def valid_url(cls, url):
        urlcomps = urlparse(url)
        return urlcomps.scheme == 'file'
