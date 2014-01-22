import os
import logging
import tarfile
import hashlib
from collections import namedtuple
from urlparse import urlparse
import toml

from zinc.formats import Formats
from zinc.models import ZincModel, ZincIndex, ZincCatalogConfig
from zinc.tasks.bundle_update import ZincBundleUpdateTask
from zinc.coordinators import coordinator_for_url
from zinc.storages import storage_for_url
from zinc.catalog import ZincCatalog
from zinc.defaults import defaults
import zinc.helpers as helpers
import zinc.utils as utils

log = logging.getLogger(__name__)


class ZincClientConfig(ZincModel):

    VARS = 'vars'
    ENV = 'env'

    def __init__(self, d=None, **kwargs):
        super(ZincClientConfig, self).__init__(**kwargs)
        self._d = d

    @classmethod
    def from_bytes(cls, b, mutable=True):
        d = toml.loads(b)
        return cls.from_dict(d, mutable=mutable)

    @classmethod
    def from_dict(cls, d, mutable=True):
        replaced = cls._replace_vars(d, d.get(cls.VARS) or dict())
        zincConfig = cls(replaced, mutable=mutable)
        return zincConfig

    @classmethod
    def _replace_vars(cls, indict, vars):
        # TODO: this could probably be a filter or something
        outdict = dict()
        for key, value in indict.iteritems():
            if isinstance(value, dict):
                outdict[key] = cls._replace_vars(value, vars)
            elif isinstance(value, basestring) \
                    and (value.startswith(cls.VARS + ':')
                         or value.startswith(cls.ENV + ':')):
                if value.startswith(cls.VARS + ':'):
                    varname = value[len(cls.VARS) + 1:]
                    var = vars[varname]
                elif value.startswith(cls.ENV + ':'):
                    varname = value[len(cls.ENV) + 1:]
                    var = os.environ[varname]
                outdict[key] = var
            else:
                outdict[key] = value
        return outdict

    @property
    def vars(self):
        return self._d.get('vars')

    @property
    def bookmarks(self):
        return self._d.get('bookmark')

    @property
    def coordinators(self):
        return self._d.get('coordinator')

    @property
    def storages(self):
        return self._d.get('storage')


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


def update_distribution(catalog, distro_name, bundle_name, version,
                        save_previous=True):
    catalog.update_distribution(distro_name, bundle_name, version,
                                save_previous=save_previous)


def delete_distribution(catalog, distribution_name, bundle_name,
                        delete_previous=True):
    catalog.delete_distribution(distribution_name, bundle_name,
                                delete_previous=delete_previous)


################################################################################


class ZincVerificationError(Exception):
    pass


def _verify_archive(manifest, fileobj=None, flavor=None, check_shas=True):

    assert fileobj

    if not check_shas:
        log.warning('Skipping SHA digest verification for archive members.')

    all_files = manifest.get_all_files(flavor=flavor)

    tar = tarfile.open(fileobj=fileobj)

    # Note: getmembers and getnames return objects in the same order
    members = tar.getmembers()
    member_names = tar.getnames()

    errors = list()

    for file in all_files:
        sha = manifest.sha_for_file(file)
        format, info = manifest.get_format_info_for_file(file, preferred_formats=defaults['catalog_preferred_formats'])
        target_member_name = helpers.append_file_extension_for_format(sha, format)
        if target_member_name not in member_names:
            errors.append(ZincVerificationError('File \'%s\' not found in %s.' % (target_member_name, helpers.make_bundle_descriptor(manifest.bundle_name, manifest.version, flavor=flavor))))
        else:
            member = members[member_names.index(target_member_name)]
            if check_shas:
                f = tar.extractfile(member)
                b = f.read()
                f.close()
                if format == Formats.GZ:
                    b = utils.gunzip_bytes(b)
                digest = hashlib.sha1(b).hexdigest()
                if digest != sha:
                    errors.append(ZincVerificationError('File \'%s\' digest does not match: %s.' % (target_member_name, digest)))
            else:
                # check length only
                if info['size'] != member.size:
                    errors.append(ZincVerificationError('File \'%s\' has size %d, expected %d.' % (target_member_name, info['size'], member.size)))

    tar.close()

    return errors


def verify_bundle(catalog, manifest=None, bundle_name=None, version=None,
                  distro=None, check_shas=True, should_lock=False, **kwargs):

    assert catalog
    assert manifest or bundle_name
    if bundle_name is not None:
        assert version or distro

    if not check_shas:
        log.warning('Skipping SHA digest verification for bundle files.')

    if manifest is None:
        if version is None:
            index = catalog.get_index()
            index.version_for_bundle(bundle_name, distro)
        manifest = catalog.get_manifest(bundle_name, version)

    ## Check individual files

    for path, info in manifest.files.iteritems():
        sha = manifest.sha_for_file(path)

        ## Note: it's important to used the reference in kwargs directly
        ##  or else changes won't be propagated to the calling code

        if kwargs.get('verified_files') is not None:
            if sha in kwargs['verified_files']:
                log.debug('Skipping %s' % (sha))
                continue
            else:
                kwargs['verified_files'].add(sha)

        try:
            meta = catalog._get_file_info(sha)
            if meta is None:
                raise ZincVerificationError('File %s not exist' % (sha))

            format = meta['format']
            meta_size = meta['size']
            manifest_size = info['formats'][format]['size']
            log.debug("file=%s format=%s meta_size=%s manifest_size=%s" % (sha, format, meta_size, manifest_size))
            if  meta_size != manifest_size:
                raise ZincVerificationError('File %s wrong size' % (sha))

            if check_shas:
                ext = helpers.file_extension_for_format(format)
                with catalog._read_file(sha, ext=ext) as f:
                    b = f.read()
                    if format == Formats.GZ:
                        b = utils.gunzip_bytes(b)
                    digest = hashlib.sha1(b).hexdigest()
                    if digest != sha:
                        raise ZincVerificationError('File %s wrong hash' % (sha))

            log.info('File %s OK' % (sha))

        except ZincVerificationError as e:
            log.error(e)

    ## Check archives

    flavors = list(manifest.flavors)
    flavors.append(None)  # no flavor

    for flavor in flavors:
        archive_name = catalog.path_helper.archive_name(manifest.bundle_name, manifest.version, flavor=flavor)
        try:
            # TODO: private reference to _get_archive_info
            meta = catalog._get_archive_info(manifest.bundle_name, manifest.version, flavor=flavor)
            if meta is None:
                if len(manifest.get_all_files(flavor=flavor)) == 1:
                    # If there is only 1 file in the bundle there should not be
                    # an archive
                    continue
                elif flavor is None and len(flavors) > 1:
                    # If there is more than 1 flavor, we usually don't need the
                    # master archive. This is probably OK, but warn anyway.
                    log.warn('Archive %s not found.' % (archive_name))
                    continue
                else:
                    raise ZincVerificationError('Archive %s not found.' % (archive_name))

            with catalog._read_archive(manifest.bundle_name, manifest.version, flavor=flavor) as a:
                archive_errors = _verify_archive(manifest, fileobj=a, flavor=flavor, check_shas=check_shas)
                if len(archive_errors) > 0:
                    for archive_err in archive_errors:
                        log.error(archive_err)
                else:
                    log.info('Archive %s OK' % (archive_name))

        except ZincVerificationError as e:
            log.error(e)


def verify_catalog(catalog, should_lock=False, **kwargs):

    errors = []
    index = catalog.get_index()
    manifests = []
    ph = catalog.path_helper

    # TODO: fix private ref to _bundle_info_by_name
    for (bundle_name, bundle_info) in index._bundle_info_by_name.iteritems():
        for version in bundle_info['versions']:
            manifest_name = ph.manifest_name(bundle_name, version)
            log.info("Loading %s" % (manifest_name))
            manifest = catalog.get_manifest(bundle_name, version)
            if manifest is None:
                errors.append("manifest not found: %s" % (manifest_name))
                continue
            manifests.append(manifest)

    verified_files = set()
    for manifest in manifests:
        log.info("Verifying %s-%d" % (manifest.bundle_name, manifest.version))
        verify_bundle(catalog, manifest=manifest, verified_files=verified_files)

    return errors


################################################################################


def clone_bundle(catalog, bundle_name, version, root_path=None, bundle_dir_name=None, flavor=None):

    assert catalog
    assert bundle_name
    assert version

    if root_path is None:
        root_path = '.'

    if bundle_dir_name is None:
        bundle_id = helpers.make_bundle_id(catalog.id, bundle_name)
        bundle_dir_name = helpers.make_bundle_descriptor(bundle_id, version,
                                                         flavor=flavor)

    manifest = catalog.manifest_for_bundle(bundle_name, version)

    if manifest is None:
        raise Exception("manifest not found: %s-%d" % (bundle_name, version))

    if flavor is not None and flavor not in manifest.flavors:
        raise Exception("manifest does not contain flavor '%s'" % (flavor))

    all_files = manifest.get_all_files(flavor=flavor)

    root_dir = os.path.join(root_path, bundle_dir_name)
    utils.makedirs(root_dir)

    for file in all_files:
        dst_path = os.path.join(root_dir, file)

        formats = manifest.formats_for_file(file)
        sha = manifest.sha_for_file(file)

        utils.makedirs(os.path.dirname(dst_path))

        if formats.get(Formats.RAW) is not None:
            format = Formats.RAW
        elif formats.get(Formats.GZ) is not None:
            format = Formats.GZ
        else:
            format = None

        ext = helpers.file_extension_for_format(format)
        with catalog._read_file(sha, ext=ext) as infile:
            b = infile.read()
            if format == Formats.GZ:
                b = utils.gunzip_bytes(b)
            with open(dst_path, 'w+b') as outfile:
                outfile.write(b)

        log.info("Exported %s --> %s" % (sha, dst_path))

    log.info("Exported %d files to '%s'" % (len(all_files), root_dir))


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

    CatalogRefSplitResult = namedtuple('CatalogRefSplitResult',
                                       'service catalog')
    CatalogInfo = namedtuple('CatalogInfo', 'id loc')

    urlcomps = urlparse(catalog_ref)
    if urlcomps.scheme in ('http', 'https'):
        catalog_id = os.path.split(urlcomps.path)[-1]
        service = catalog_ref[:-len(catalog_id)]
        return CatalogRefSplitResult(service, CatalogInfo(catalog_id, None))

    elif urlcomps.scheme in ('file', ''):
        return CatalogRefSplitResult(catalog_ref, CatalogInfo(None, '.'))


## TODO: fix cloning between this and zinc.services.simple
def create_catalog(catalog_id=None, storage_info=None):
    assert catalog_id
    assert storage_info

    storage_class = storage_for_url(storage_info['url'])
    storage = storage_class(**storage_info)
    catalog_storage = storage.bind_to_catalog(id=catalog_id)

    catalog_storage.puts(defaults['catalog_config_name'],
                         ZincCatalogConfig().to_bytes())

    catalog_storage.puts(defaults['catalog_index_name'],
                         ZincIndex(catalog_id).to_bytes())

    catalog = ZincCatalog(storage=catalog_storage)
    catalog.save()


def get_service(service_url=None, coordinator_info=None, storage_info=None, **kwargs):

    if service_url is not None:

        urlcomps = urlparse(service_url)
        if urlcomps.scheme in ('http', 'https'):
            _catalog_connection_get_http(service_url)

            from zinc.services.web import WebServiceConsumer
            return WebServiceConsumer(service_url)

        elif urlcomps.scheme in ('file', ''):
            if urlcomps.scheme == '':
                # assume it's a path and convert a file URL
                file_url = 'file://%s' % (utils.canonical_path(service_url))
            else:
                file_url = service_url

            from zinc.services.simple import SimpleServiceConsumer
            return SimpleServiceConsumer(file_url)

    elif coordinator_info is not None and storage_info is not None:

        coord_class = coordinator_for_url(coordinator_info['url'])
        coord = coord_class(**coordinator_info)

        storage_class = storage_for_url(storage_info['url'])
        storage = storage_class(**storage_info)

        from zinc.services import CustomServiceConsumer
        return CustomServiceConsumer(coordinator=coord, storage=storage)

    raise NotImplementedError()


def connect(service_url=None, coordinator_info=None, storage_info=None, **kwargs):
    return get_service(service_url=service_url,
                       coordinator_info=coordinator_info,
                       storage_info=storage_info, **kwargs)
