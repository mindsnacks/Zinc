import ConfigParser
import logging

from zinc.defaults import defaults
from zinc.models import ZincIndex

from zinc.tasks.bundle_update import ZincBundleUpdateTask

class ZincClientConfig(object):

    def __init__(self, bookmarks=None):
        self._bookmarks = bookmarks or dict()

    @property
    def bookmarks(self):
        return self._bookmarks

    @classmethod
    def from_path(cls, path):
        config = ConfigParser.ConfigParser()
        config.read(path)

        bookmarks = dict(config.items('bookmarks'))

        zincConfig = ZincClientConfig(
                bookmarks=bookmarks)

        return zincConfig


class ZincClient(object):

    def __init__(self, catalog):
        assert catalog

        self._catalog = catalog

    @property
    def catalog(self):
        return self._catalog

    def create_bundle_version(self, bundle_name, src_dir, 
            flavor_spec=None, force=False, skip_master_archive=False):
    
        task = ZincBundleUpdateTask()
        task.catalog = self.catalog
        task.bundle_name = bundle_name
        task.src_dir = src_dir
        task.flavor_spec = flavor_spec
        task.skip_master_archive = skip_master_archive
        task.force = force
        return task.run()
    
