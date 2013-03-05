import os
from urlparse import urlparse
from lockfile import FileLock

from zinc.catalog import CatalogCoordinator
from zinc.utils import *

class FilesystemCatalogCoordinator(CatalogCoordinator):

    def _index_lock_path(self):
        return os.path.join(self.path, '.index')

    def get_index_lock(self):
        return FileLock(self._index_lock_path())

    @property
    def path(self):
        return urlparse(self.url).path

    # TODO: this sucks
    def _after_init(self):
        makedirs(os.path.join(self.path, self._ph.objects_dir))
        makedirs(os.path.join(self.path, self._ph.manifests_dir))
        makedirs(os.path.join(self.path, self._ph.archives_dir))

    @classmethod
    def validate_url(cls, url):
        urlcomps = urlparse(url)
        return urlcomps.scheme == 'file'

