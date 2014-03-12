from __future__ import absolute_import

import os
import logging
import tarfile
import hashlib
from collections import namedtuple
from urlparse import urlparse
import toml
import json

import zinc.helpers as helpers
import zinc.utils as utils
from .catalog import ZincCatalog
from .defaults import defaults
from .coordinators import coordinator_for_url
from .formats import Formats
from .models import ZincModel, ZincIndex, ZincCatalogConfig
from .storages import storage_for_url
from .tasks.bundle_update import ZincBundleUpdateTask
from .utils import enum, memoized

log = logging.getLogger(__name__)

SymbolicBundleVersions = utils.enum(
        ALL=':all',
        UNREFERENCED=':unreferenced',
        LATEST=':latest')

# TODO: why doesn't this work?
#SymbolicSingleBundleVersions = utils.enum(
#        LATEST=SymbolicBundleVersions.LATEST)
SymbolicSingleBundleVersions = utils.enum(
        LATEST=':latest')

BundleVersionDistroPrefix = '@'


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

OutputType = enum(PRETTY='pretty', JSON='json')


class _Result(object):

    def __init__(self, pretty=None):
        self._pretty = pretty or str

    def to_dict(self):
        raise NotImplementedError()

    def format(self, fmt):
        if fmt == OutputType.JSON:
            return json.dumps(self.to_dict())
        elif fmt == OutputType.PRETTY:
            return self._pretty(self)
        else:
            raise NotImplementedError()


class DictResult(_Result):

    def __init__(self, d, **kwargs):
        super(DictResult, self).__init__(**kwargs)
        self._dict = d

    def to_dict(self):
        return self._dict

    def __str__(self):
        return str(self._dict)

    def __getitem__(self, k):
        return self._dict[k]


MessageTypes = enum(
    INFO='info',
    WARNING='warning',
    ERROR='error')


class Message(_Result):

    def __init__(self, type, text, **kwargs):
        super(Message, self).__init__(**kwargs)
        self._type = type
        self._text = text

    @classmethod
    def info(cls, s):
        return cls(MessageTypes.INFO, s)

    @classmethod
    def warn(cls, s):
        return cls(MessageTypes.WARNING, s)

    @classmethod
    def error(cls, s):
        return cls(MessageTypes.ERROR, s)

    @property
    def text(self):
        return self._text

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, val):
        assert val in MessageTypes
        self._type = val

    def to_dict(self):
        return {
            'message': {
                'type': self.type,
                'text': self.text,
            }
        }

    def __str__(self):
        return '[%s] %s' % (self._type, self._text)


class ResultSet(object):

    def __init__(self, items, pretty=None):
        self._items = items
        self.pretty = pretty or str

    @property
    @memoized
    def items(self):
        return self._items()

    def __iter__(self):
        return iter(self.items)

    def __str__(self):
        return str(self.items)

    def errors(self):
        return [i for i in self.items if isinstance(i, Message) and i.type == MessageTypes.ERROR]

    # TODO: reimplement
    #def dump(self, fmt):
    #    if fmt == OutputType.JSON:
    #        return json.dumps(list(self.items))
    #    elif fmt == OutputType.PRETTY:
    #        return string.join([self.pretty(x) for x in self], '\n')
    #    else:
    #        raise NotImplementedError()


################################################################################

def catalog_list(catalog, distro=None, print_versions=True, **kwargs):

    index = catalog.get_index()

    def pretty_without_versions(result):
        return "%s" % (result['bundle_name'])

    def pretty_with_versions(result):
        distros = index.distributions_for_bundle_by_version(result['bundle_name'])
        versions = index.versions_for_bundle(result['bundle_name'])
        version_strings = list()
        for version in versions:
            version_string = str(version)
            if distros.get(version) is not None:
                distro_string = "(%s)" % (", ".join(sorted(distros.get(version))))
                version_string += '=' + distro_string
                version_strings.append(version_string)

        final_version_string = "[%s]" % (", ".join(version_strings))
        return "%s %s" % (result['bundle_name'], final_version_string)

    pretty = pretty_with_versions if print_versions else pretty_without_versions

    def results():
        bundle_names = sorted(index.bundle_names())
        for bundle_name in bundle_names:
            d = dict()
            if distro and distro not in index.distributions_for_bundle(bundle_name):
                continue
            d['bundle_name'] = bundle_name
            d['versions'] = index.versions_for_bundle(bundle_name)
            d['distros'] = index.distributions_for_bundle(bundle_name)
            yield DictResult(d, pretty=pretty)

    return ResultSet(results)


def bundle_list(catalog, bundle_name, version_ish, print_sha=False, flavor_name=None):

    version = _resolve_single_bundle_version(catalog, bundle_name, version_ish)
    manifest = catalog.manifest_for_bundle(bundle_name, version=version)

    def pretty(r):
        if print_sha:
            return "%s sha=%s" % (r['file'], r['sha'])
        else:
            return "%s" % (r['file'])

    def results():
        all_files = sorted(manifest.get_all_files(flavor=flavor_name))
        for f in all_files:
            d = {
                'file':  f,
                'sha': manifest.sha_for_file(f)
            }
            yield DictResult(d, pretty=pretty)

    return ResultSet(results)


def bundle_verify(catalog, bundle_name, version_ish, check_shas=True,
        should_lock=False, **kwargs):

    version = _resolve_single_bundle_version(catalog, bundle_name, version_ish)
    manifest = catalog.get_manifest(bundle_name, version)

    def results():

        for result in _verify_bundle_with_manifest(catalog, manifest,
                                                   check_shas=check_shas,
                                                   should_lock=should_lock,
                                                   **kwargs):
            yield result

    return ResultSet(results)


def verify_catalog(catalog, should_lock=False, **kwargs):

    index = catalog.get_index()
    manifests = list()
    ph = catalog.path_helper

    def results():

        # TODO: fix private ref to _bundle_info_by_name
        for (bundle_name, bundle_info) in index._bundle_info_by_name.iteritems():
            for version in bundle_info['versions']:
                manifest_name = ph.manifest_name(bundle_name, version)
                yield Message.info("Loading %s" % (manifest_name))
                manifest = catalog.get_manifest(bundle_name, version)
                if manifest is None:
                    yield Message.error("manifest not found: %s" % (manifest_name))
                    continue
                manifests.append(manifest)

        verified_files = set()
        for manifest in manifests:
            yield Message.info("Verifying %s-%d" % (manifest.bundle_name,
                                                    manifest.version))
            for result in _verify_bundle_with_manifest(catalog, manifest,
                                                       verified_files=verified_files):
                yield result

    return ResultSet(results)


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


def delete_bundle_versions(catalog, bundle_name, version_ish):
    version_list = _resolve_multiple_bundle_versions(catalog, bundle_name, version_ish)
    with catalog.lock():
        for version in version_list:
            catalog.delete_bundle_version(bundle_name, version)


def update_distribution(catalog, distro_name, bundle_name, version,
                        save_previous=True):
    catalog.update_distribution(distro_name, bundle_name, version,
                                save_previous=save_previous)


def delete_distribution(catalog, distribution_name, bundle_name,
                        delete_previous=True):
    catalog.delete_distribution(distribution_name, bundle_name,
                                delete_previous=delete_previous)


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


################################################################################
## Verification Helpers

def _verify_bundle_with_manifest(catalog, manifest, check_shas=True,
        should_lock=False, **kwargs):

    if not check_shas:
        yield Message.warn('Skipping SHA digest verification for bundle files.')

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

        meta = catalog._get_file_info(sha)
        if meta is None:
            #run.append(VerificationError('File %s not exist' % (sha)))
            yield Message.error('File %s not exist' % (sha))
            continue

        format = meta['format']
        meta_size = meta['size']
        manifest_size = info['formats'][format]['size']
        log.debug("file=%s format=%s meta_size=%s manifest_size=%s" % (sha, format, meta_size, manifest_size))
        if  meta_size != manifest_size:
            yield Message.error('File %s wrong size' % (sha))
            continue

        if check_shas:
            ext = helpers.file_extension_for_format(format)
            with catalog._read_file(sha, ext=ext) as f:
                b = f.read()
                if format == Formats.GZ:
                    b = utils.gunzip_bytes(b)
                digest = hashlib.sha1(b).hexdigest()
                if digest != sha:
                    yield Message.error('File %s wrong hash' % (sha))
                    continue

        yield Message.info('File %s OK' % (sha))

    ## Check archives

    flavors = list(manifest.flavors)
    flavors.append(None)  # no flavor

    for flavor in flavors:
        archive_name = catalog.path_helper.archive_name(manifest.bundle_name, manifest.version, flavor=flavor)
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
                #log.warn('Archive %s not found.' % (archive_name))
                Message.warn('Archive %s not found.' % (archive_name))
                continue
            else:
                yield Message.error('Archive %s not found.' % (archive_name))

        for result in _verify_archive(catalog, manifest, flavor=flavor, check_shas=check_shas):
            yield result


def _verify_archive(catalog, manifest, flavor=None, check_shas=True):

    if not check_shas:
        yield Message.warn('Skipping SHA digest verification for archive members.')

    archive_name = catalog.path_helper.archive_name(manifest.bundle_name, manifest.version, flavor=flavor)
    all_files = manifest.get_all_files(flavor=flavor)

    with catalog._read_archive(manifest.bundle_name, manifest.version, flavor=flavor) as fileobj:

        tar = tarfile.open(fileobj=fileobj)

        # Note: getmembers and getnames return objects in the same order
        members = tar.getmembers()
        member_names = tar.getnames()

        found_error = False

        for file in all_files:
            sha = manifest.sha_for_file(file)
            format, info = manifest.get_format_info_for_file(file, preferred_formats=defaults['catalog_preferred_formats'])
            target_member_name = helpers.append_file_extension_for_format(sha, format)
            if target_member_name not in member_names:
                found_error = True
                yield Message.error('File \'%s\' not found in %s.' % (target_member_name, helpers.make_bundle_descriptor(manifest.bundle_name, manifest.version, flavor=flavor)))
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
                        found_error = True
                        yield Message.error('File \'%s\' digest does not match: %s.' % (target_member_name, digest))
                else:
                    # check length only
                    if info['size'] != member.size:
                        found_error = True
                        yield Message.error('File \'%s\' has size %d, expected %d.' % (target_member_name, info['size'], member.size))

        tar.close()

    if not found_error:
        yield Message.info('Archive %s OK' % (archive_name))


def _resolve_single_bundle_version(catalog, bundle_name, version_ish):

    if isinstance(version_ish, int):
        version = version_ish

    elif version_ish == SymbolicBundleVersions.LATEST:
        index = catalog.get_index()
        version = index.versions_for_bundle(bundle_name)[-1]

    elif version_ish.startswith('@'):
        source_distro = version_ish[1:]
        version = catalog.index.version_for_bundle(bundle_name, source_distro)

    return version


################################################################################
## Version Resolution Helpers

def _resolve_multiple_bundle_versions(catalog, bundle_name, version_ish):

    if version_ish == SymbolicBundleVersions.ALL:
        index = catalog.get_index()
        versions = index.versions_for_bundle(bundle_name)

    elif version_ish == SymbolicBundleVersions.UNREFERENCED:
        index = catalog.get_index()
        all_versions = index.versions_for_bundle(bundle_name)
        referenced_versions = catalog.index.distributions_for_bundle_by_version(bundle_name).keys()
        versions = [v for v in all_versions if v not in referenced_versions]

    single_version = _resolve_single_bundle_version(catalog, bundle_name, version_ish)
    if single_version is not None:
        versions = [single_version]

    return versions
