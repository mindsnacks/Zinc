import os
import json
from urlparse import urlparse

from zinc.catalog import CatalogCoordinator
from zinc.defaults import defaults
from zinc.models import ZincIndex
from zinc.utils import *

class FilesystemCatalogCoordinator(CatalogCoordinator):

    ### TODO: fs-based locking

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

    def read_index(self):
        path = self._ph.path_for_index()
        bytes = self._storage.get(path)
        return ZincIndex.from_bytes(bytes)

    def save_index(self, index):
        index.write(self._index_path(), True)

    def load_path(self, rel_path):
        return self._storage.get(rel_path)

    def write_manifest(self, manifest, gzip=True):
        subpath = self._ph.path_for_manifest(manifest)
        bytes = manifest.to_bytes()
        self._storage.put(subpath, bytes)

