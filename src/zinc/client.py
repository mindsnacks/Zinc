import ConfigParser
import logging
from urlparse import urlparse

from zinc.defaults import defaults
from zinc.utils import *
from zinc.helpers import *

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

    def __init__(self, service):
        assert service

        self._service = service 

    @property
    def service(self):
        return self._service

    def catalog_index(self):
        return self._service.get_index()

    def create_bundle_version(self, bundle_name, src_dir, 
            flavor_spec=None, force=False, skip_master_archive=False):

        from zinc.coordinators.filesystem import FilesystemCatalogCoordinator
        from zinc.storages.filesystem import FilesystemStorageBackend
        from zinc.catalog import ZincCatalog

        # TODO: colossal hack here...
        url = self.service.url
        storage = FilesystemStorageBackend(url=url)
        coordinator = FilesystemCatalogCoordinator(url=url, storage=storage)
        catalog = ZincCatalog(coordinator=coordinator)
    
        task = ZincBundleUpdateTask()
        task.catalog = catalog
        task.bundle_name = bundle_name
        task.src_dir = src_dir
        task.flavor_spec = flavor_spec
        task.skip_master_archive = skip_master_archive
        task.force = force
        return task.run()

################################################################################

def _catalog_connection_get_api_version(url):
    ZINC_VERSION_HEADER = 'x-zinc-api-version'
    resp = requests.head(url)
    api_version = resp.headers.get(ZINC_VERSION_HEADER)
    if api_version is None:
        raise Exception("Unknown Zinc API - '%s' header not found" %
                (ZINC_VERSION_HEADER))
    return api_version

def _catalog_connection_get_http(url):
    ZINC_SUPPORTED_API_VERSIONS = ('1.0')
    api_version = _catalog_connection_get_api_version(url)
    if api_version not in ZINC_SUPPORTED_API_VERSIONS:
        raise Exception("Unsupported Zinc API version '%s'" % (api_version))
    else:
        logging.debug("Found Zinc API %s" % (api_version))


def connect(catalog_ref):
    urlcomps = urlparse(catalog_ref)
    if urlcomps.scheme in ('http', 'https'):
        _catalog_connection_get_http(catalog_ref)           
    elif urlcomps.scheme in ('file', ''):
        if urlcomps.scheme == '':
            # assume it's a path and convert a file URL
            url = 'file://%s' % (canonical_path(catalog_ref))
        else:
            url = catalog_ref

        from zinc.services.simple import SimpleService
        service = SimpleService(catalog_ref)

    if service is not None:
        return ZincClient(service)

    # TODO: error, exception
    return None


