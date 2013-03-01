import os
import json
from urlparse import urlparse

from zinc.catalog import CatalogCoordinator
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



