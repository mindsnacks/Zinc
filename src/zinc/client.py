import toml
from urlparse import urlparse
from collections import namedtuple
import logging

from zinc.utils import *
from zinc.helpers import *
from zinc.models import ZincModel
from zinc.tasks.bundle_update import ZincBundleUpdateTask
from zinc.coordinators import coordinator_for_url
from zinc.storages import storage_for_url

log = logging.getLogger(__name__)


class ZincClientConfig(ZincModel):

    VARS = 'vars'

    def __init__(self, d=None, **kwargs):
        super(ZincClientConfig, self).__init__(**kwargs)
        self._d = d

    @classmethod
    def from_bytes(cls, b, mutable=True):
        d = toml.loads(b)
        return cls.from_dict(d, mutable=mutable)

    @classmethod
    def from_dict(cls, d, mutable=True):
        replaced = cls._replace_vars(d, d[cls.VARS])
        zincConfig = cls(replaced, mutable=mutable)
        return zincConfig

    @classmethod
    def _replace_vars(cls, indict, vars):
        # TODO: this could probably be a filter or something
        outdict = dict()
        for key, value in indict.iteritems():
            if isinstance(value, dict):
                outdict[key] = cls._replace_vars(value, vars)
            elif isinstance(value, basestring) and value.startswith(cls.VARS + '.'):
                varname = value[len(cls.VARS) + 1:]
                var = vars[varname]
                outdict[key] = var
            else:
                outdict[key] = value
        return outdict

    @property
    def vars(self):
        return self._d.get('vars')

    @property
    def services(self):
        return self._d.get('services')

    @property
    def bookmarks(self):
        return self._d.get('bookmarks')


################################################################################

def create_bundle_version(catalog, bundle_name, src_dir, flavor_spec=None,
                          force=False, skip_master_archive=False):

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
    import requests
    ZINC_VERSION_HEADER = 'x-zinc-api-version'
    resp = requests.head(url, allow_redirects=False)
    # TODO is preventing redirects what we want?
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
        log.debug("Found Zinc API %s" % (api_version))


def catalog_ref_split(catalog_ref):

    CatalogRefSplitResult = namedtuple(
            'CatalogRefSplitResult', 'service catalog')
    CatalogInfo = namedtuple('CatalogInfo', 'id loc')

    urlcomps = urlparse(catalog_ref)
    if urlcomps.scheme in ('http', 'https'):
        catalog_id = os.path.split(urlcomps.path)[-1]
        service = catalog_ref[:-len(catalog_id)]
        return CatalogRefSplitResult(service, CatalogInfo(catalog_id, None))

    elif urlcomps.scheme in ('file', ''):
        return CatalogRefSplitResult(catalog_ref, CatalogInfo(None, '.'))


def connect(service_url=None, coordinator_url=None, storage_url=None, **kwargs):

    if service_url is not None:

        urlcomps = urlparse(service_url)
        if urlcomps.scheme in ('http', 'https'):
            _catalog_connection_get_http(service_url)

            from zinc.services.web import WebServiceConsumer
            return WebServiceConsumer(service_url)

        elif urlcomps.scheme in ('file', ''):
            if urlcomps.scheme == '':
                # assume it's a path and convert a file URL
                file_url = 'file://%s' % (canonical_path(service_url))
            else:
                file_url = service_url

            from zinc.services.simple import SimpleServiceConsumer
            return SimpleServiceConsumer(file_url)

    elif coordinator_url is not None and storage_url is not None:

        coord_class = coordinator_for_url(coordinator_url)
        coord = coord_class(url=coordinator_url, **kwargs)

        storage_class = storage_for_url(storage_url)
        storage = storage_class(url=storage_url, **kwargs)

        from zinc.services import CustomServiceConsumer
        return CustomServiceConsumer(coordinator=coord, storage=storage)



