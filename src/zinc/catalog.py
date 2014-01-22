import os
import logging
from functools import wraps
from urlparse import urlparse
import tempfile
from typecheck import accepts, Self

from zinc.models import ZincIndex, ZincManifest, ZincCatalogConfig, ZincFlavorSpec
from zinc.defaults import defaults
from zinc.formats import Formats
import zinc.helpers as helpers
import zinc.utils as utils

log = logging.getLogger(__name__)


################################################################################


# TODO: rename to CatalogPathHelper?
class ZincCatalogPathHelper(object):

    def __init__(self, format='1'):
        if format != defaults['zinc_format']:
            raise Exception("Incompatible format %s" % (format))
        self._format = format

    @property
    def format(self):
        return self._format

    @property
    def manifests_dir(self):
        return "manifests"

    @property
    def archives_dir(self):
        return "archives"

    @property
    def objects_dir(self):
        return "objects"

    @property
    def config_dir(self):
        return "config"

    @property
    def config_flavorspec_dir(self):
        return os.path.join(self.config_dir, "flavorspecs")

    def path_for_index(self):
        return defaults['catalog_index_name']

    def manifest_name(self, bundle_name, version):
        return "%s-%d.json" % (bundle_name, version)

    def path_for_manifest_for_bundle_version(self, bundle_name, version):
        manifest_filename = self.manifest_name(bundle_name, version)
        manifest_path = os.path.join(self.manifests_dir, manifest_filename)
        return manifest_path

    def path_for_manifest(self, manifest):
        return self.path_for_manifest_for_bundle_version(
                manifest.bundle_name, manifest.version)

    def path_for_file_with_sha(self, sha, ext=None, format=None):

        if ext is not None and format is not None:
            raise Exception(
                    "Should specify either `ext` or `format`, not both.")

        if format is not None:
            ext = helpers.file_extension_for_format(format)
        subdir = os.path.join(self.objects_dir, sha[0:2], sha[2:4])
        file = sha
        if ext is not None:
            file = file + '.' + ext
        return os.path.join(subdir, file)

    def archive_name(self, bundle_name, version, flavor=None):
        if flavor is None:
            return "%s-%d.tar" % (bundle_name, version)
        else:
            return "%s-%d~%s.tar" % (bundle_name, version, flavor)

    def path_for_archive_for_bundle_version(
            self, bundle_name, version, flavor=None):
        archive_filename = self.archive_name(bundle_name, version, flavor=flavor)
        archive_path = os.path.join(self.archives_dir, archive_filename)
        return archive_path

    def path_for_flavorspec_name(self, flavorspec_name):
        filename = '%s.json' % flavorspec_name
        return os.path.join(self.config_flavorspec_dir, filename)


################################################################################

class ZincAbstractCatalog(object):

    def get_index(self):
        """
        Returns an *immutable* copy of the catalog index.
        """
        raise NotImplementedError()

    def get_manifest(self, bundle_name, version):
        """
        Returns an *immutable* copy of the manifest for the specified
        `bundle_name` and version`.
        """
        raise NotImplementedError()

    def update_bundle(self, new_manifest):
        raise NotImplementedError()

    # special
    def import_path(self, src_path):
        raise NotImplementedError()

    def delete_bundle_version(self, bundle_name, version):
        raise NotImplementedError()

    def update_distribution(self, distribution_name, bundle_name, bundle_version, save_previous=True):
        raise NotImplementedError()

    def delete_distribution(self, distribution_name, bundle_name):
        raise NotImplementedError()

    def verify(self):
        raise NotImplementedError()

    def clean(self, **kwargs):
        raise NotImplementedError()

    ### Non-abstract methods

    def manifest_for_bundle(self, bundle_name, version=None):
        """
        Get a manifest for bundle. If version is not specified, it gets the
        manifest with the highest version number.
        """
        index = self.get_index()
        all_versions = index.versions_for_bundle(bundle_name)
        if version is None and len(all_versions) > 0:
            version = all_versions[-1]
        elif version not in all_versions:
            return None  # throw exception?
        return self.get_manifest(bundle_name, version)

    def manifest_for_bundle_descriptor(self, bundle_descriptor):
        """
        Convenience method to get a manifest by bundle_descriptor.
        """
        return self.manifest_for_bundle(
            helpers.bundle_id_from_bundle_descriptor(bundle_descriptor),
            helpers.bundle_version_from_bundle_descriptor(bundle_descriptor))

    def bundle_descriptors(self):
        bundle_descriptors = []
        index = self.get_index()
        for bundle_name in index.bundle_names():
            for version in index.versions_for_bundle(bundle_name):
                bundle_descriptors.append("%s-%d" % (bundle_name, version))
                manifest = self.manifest_for_bundle(bundle_name, version)
                if manifest is None:
                    log.warn('Could not load manifest for %s-%d' % (bundle_name, version))
                    continue
                for flavor in manifest.flavors:
                    bundle_descriptors.append("%s-%d~%s" %
                            (bundle_name, version, flavor))
        return bundle_descriptors


################################################################################


class ZincCatalogLock(object):

    def __init__(self, catalog, lock):
        self._catalog = catalog
        self._lock = lock

    def __enter__(self):
        self._lock.acquire()
        self._catalog._reload()

    def __exit__(self, exc_type, exc_value, traceback):
        self._catalog.save()
        self._lock.release()

    def is_locked(self):
        return self._lock.is_locked()


class ZincCatalog(ZincAbstractCatalog):

    def __init__(self, storage=None, coordinator=None, path_helper=None,
                 lock_timeout=None, **kwargs):
        assert storage

        super(ZincCatalog, self).__init__(**kwargs)

        self._coordinator = coordinator
        self._storage = storage

        self._ph = path_helper or ZincCatalogPathHelper()
        self._manifests = {}
        self.lock_timeout = lock_timeout or defaults['catalog_lock_timeout']

        self._reload()

        if self._coordinator is not None:
            self._lock = ZincCatalogLock(self,
                    self._coordinator.get_index_lock(
                        domain=self.id,
                        timeout=lock_timeout))

    def lock(self):
        assert self._lock
        return self._lock

    ### Properties ###

    @property
    def url(self):
        return self._storage.url

    @property
    def path(self):
        return urlparse(self.url).path

    @property
    def id(self):
        return self.index.id

    @property
    def path_helper(self):
        return self._ph

    def format(self):
        return self.index.format

    ### General Internal Methods ###

    def _reload(self):

        self.index = self._read_index()
        if self.index.format != defaults['zinc_format']:
            raise Exception("Incompatible format %s" % (self.index.format))

        self._read_config_file()

    def _read_config_file(self):
        #log.warn('reimplement config loading')
        self.config = ZincCatalogConfig()
        #config_path = pjoin(self.path, defaults['catalog_config_name'])
        #self.config = load_config(config_path)

    def _ensure_index_lock(func):
        @wraps(func)
        def with_ensure_index_lock(self, *args, **kwargs):

            assert self._coordinator

            if not self.lock().is_locked():
                with self.lock():
                    output = func(self, *args, **kwargs)
            else:
                output = func(self, *args, **kwargs)
            return output
        return with_ensure_index_lock

    ### I/O Helpers ###

    def _read(self, rel_path):
        f = self._storage.get(rel_path)
        return f.read() if f is not None else None

    def _write(self, subpath, bytes, raw=True, gzip=True, max_age=None):
        if raw:
            self._storage.puts(subpath, bytes, max_age=max_age)
        if gzip:
            self._storage.puts(subpath + '.gz', utils.gzip_bytes(bytes), max_age=max_age)

    def _read_index(self):
        subpath = self._ph.path_for_index()
        bytes = self._read(subpath)
        return ZincIndex.from_bytes(bytes)

    def _write_index(self, index, raw=True, gzip=True):
        subpath = self._ph.path_for_index()
        bytes = index.to_bytes()
        max_age = defaults['catalog_index_max_age_seconds']
        self._write(subpath, bytes, raw=raw, gzip=gzip, max_age=max_age)
        if defaults['catalog_write_legacy_index']:
            self._write('index.json', bytes, raw=raw, gzip=gzip, max_age=max_age)

    def _read_manifest(self, bundle_name, version):
        subpath = self._ph.path_for_manifest_for_bundle_version(bundle_name,
                                                                version)
        bytes = self._read(subpath)
        if bytes is not None:
            return ZincManifest.from_bytes(bytes)
        else:
            return None

    def _write_manifest(self, manifest, raw=True, gzip=True):
        subpath = self._ph.path_for_manifest(manifest)
        bytes = manifest.to_bytes()
        self._write(subpath, bytes, raw=raw, gzip=gzip)

    def _get_file_info(self, sha, preferred_formats=None):
        if preferred_formats is None:
            preferred_formats = defaults['catalog_preferred_formats']
        for format in preferred_formats:
            subpath = self._ph.path_for_file_with_sha(sha, format=format)
            meta = self._storage.get_meta(subpath)
            if meta is not None:
                return {
                    'sha': sha,
                    'size': meta['size'],
                    'format': format
                }
        return None

    def _read_file(self, sha, ext=None):
        subpath = self._ph.path_for_file_with_sha(sha, ext=ext)
        return self._storage.get(subpath)

    def _write_file(self, sha, src_path, format=None):
        format = format or Formats.RAW  # default to RAW
        if format not in defaults['catalog_valid_formats']:
            raise Exception("Invalid format '%s'." % (format))
        ext = format if format != Formats.RAW else None
        subpath = self._ph.path_for_file_with_sha(sha, ext)
        with open(src_path, 'r') as src_file:
            self._storage.put(subpath, src_file)
        return subpath

    def _get_archive_info(self, bundle_name, version, flavor=None):
        subpath = self._ph.path_for_archive_for_bundle_version(bundle_name,
                                                               version,
                                                               flavor=flavor)
        meta = self._storage.get_meta(subpath)
        return meta

    def _write_archive(self, bundle_name, version, src_path, flavor=None):
        subpath = self._ph.path_for_archive_for_bundle_version(bundle_name,
                                                               version,
                                                               flavor=flavor)
        with open(src_path, 'r') as src_file:
            self._storage.put(subpath, src_file)
        return subpath

    def _read_archive(self, bundle_name, version, flavor=None):
        subpath = self._ph.path_for_archive_for_bundle_version(bundle_name,
                                                               version,
                                                               flavor=flavor)
        return self._storage.get(subpath)

    @_ensure_index_lock
    def _reserve_version_for_bundle(self, bundle_name):
        self.index.increment_next_version_for_bundle(bundle_name)
        return self.index.next_version_for_bundle(bundle_name)

    ### "Public" Methods

    def save(self):
        self._write_index(self.index)

    def get_index(self):
        return self.index.clone(mutable=False)

    @accepts(Self(), basestring, int)
    def get_manifest(self, bundle_name, version):
        return self._read_manifest(bundle_name, version)

    @_ensure_index_lock
    @accepts(Self(), ZincManifest)
    def update_bundle(self, new_manifest):

        assert new_manifest

        existing_versions = self.index.versions_for_bundle(new_manifest.bundle_name)
        if new_manifest.version in existing_versions:
            raise ValueError("Bundle version already exists.")

        next_version = self.index.next_version_for_bundle(new_manifest.bundle_name)
        if new_manifest.version > next_version:
            raise ValueError("Unexpected manifest version.")

        ### verify all files in the filelist exist in the repo

        missing_shas = list()
        info_by_path = dict()

        for path in new_manifest.files.keys():
            sha = new_manifest.sha_for_file(path)
            file_info = self._get_file_info(sha)
            if file_info is None:
                missing_shas.append(sha)
            else:
                info_by_path[path] = file_info

        if len(missing_shas) > 0:
            # TODO: better error
            raise Exception("Missing shas: %s" % (missing_shas))

        ### TODO: verify archives?

        ### write manifest

        self._write_manifest(new_manifest)

        ### update catalog index

        self.index.add_version_for_bundle(new_manifest.bundle_name,
                                          new_manifest.version)

    @accepts(Self(), basestring)
    def import_path(self, src_path):

        sha = utils.sha1_for_path(src_path)

        file_info = self._get_file_info(sha)
        if file_info is not None:
            return file_info

        # gzip the file first, and see if it passes the compression threshhold
        # TODO: this is stupid inefficient
        with tempfile.NamedTemporaryFile() as tmp_file:
            src_path_gz = tmp_file.name
            with open(src_path) as src_file:
                tmp_file.write(utils.gzip_bytes(src_file.read()))
            tmp_file.flush()

            src_size = os.path.getsize(src_path)
            src_gz_size = os.path.getsize(src_path_gz)
            if src_size > 0 and float(src_gz_size) / src_size <= self.config.gzip_threshhold:
                final_src_path = src_path_gz
                final_src_size = src_gz_size
                format = Formats.GZ
            else:
                final_src_path = src_path
                final_src_size = src_size
                format = Formats.RAW

            imported_path = self._write_file(sha, final_src_path, format=format)

        file_info = {
            'sha':  sha,
            'size': final_src_size,
            'format': format
        }
        log.info("Imported %s --> %s" % (src_path, file_info))
        log.debug("Imported path: %s" % imported_path)
        return  file_info

    @_ensure_index_lock
    @accepts(Self(), basestring, int)
    def delete_bundle_version(self, bundle_name, version):
        self.index.delete_bundle_version(bundle_name, version)

    @_ensure_index_lock
    @accepts(Self(), basestring, basestring, int, bool)
    def update_distribution(self, distribution_name, bundle_name, bundle_version, save_previous=True):

        if save_previous:
            cur_version = self.index.version_for_bundle(bundle_name, distribution_name)
            if cur_version is not None and cur_version != bundle_version:
                prev_distro = helpers.distro_previous_name(distribution_name)
                self.index.update_distribution(prev_distro, bundle_name, cur_version)

        self.index.update_distribution(distribution_name, bundle_name, bundle_version)

    @_ensure_index_lock
    @accepts(Self(), basestring, basestring, bool)
    def delete_distribution(self, distribution_name, bundle_name, delete_previous=True):
        self.index.delete_distribution(distribution_name, bundle_name)
        if delete_previous:
            prev_distro = helpers.distro_previous_name(distribution_name)
            self.index.delete_distribution(prev_distro, bundle_name)

    def get_flavorspec_names(self):
        subpath = self.path_helper.config_flavorspec_dir
        return [os.path.splitext(p)[0] for p in self._storage.list(prefix=subpath)]

    def get_flavorspec(self, flavorspec_name):
        subpath = self._ph.path_for_flavorspec_name(flavorspec_name)
        bytes = self._read(subpath)
        return ZincFlavorSpec.from_bytes(bytes)

    def update_flavorspec_from_json_string(self, name, json_string):
        subpath = self._ph.path_for_flavorspec_name(name)
        self._write(subpath, json_string, raw=True, gzip=False)

    def update_flavorspec_from_path(self, src_path, name=None):
        with open(src_path, 'r') as src_file:
            json_string = src_file.read()
        if name is None:
            name = os.path.splitext(os.path.basename(src_path))[0]
        self.update_flavorspec_from_json_string(name, json_string)

    def delete_flavorspec(self, name):
        subpath = self._ph.path_for_flavorspec_name(name)
        self._storage.delete(subpath)

    @_ensure_index_lock
    def clean(self, dry_run=False):
        verb = 'Would remove' if dry_run else 'Removing'

        bundle_descriptors = self.bundle_descriptors()

        ### 1. scan manifests for ones that aren't in index

        dir = self._ph.manifests_dir
        for f in self._storage.list(dir):
            remove = False
            if not (f.endswith(".json") or f.endswith(".json.gz")):
                # remove stray files
                remove = True
            else:
                bundle_descr = f.split(".")[0]
                if bundle_descr not in bundle_descriptors:
                    remove = True

            if remove:
                subpath = os.path.join(dir, f)
                log.info("%s %s" % (verb, subpath))
                if not dry_run:
                    self._storage.delete(subpath)

        ### 2. scan archives for ones that aren't in index

        dir = self._ph.archives_dir
        for f in self._storage.list(dir):
            remove = False
            if not (f.endswith(".tar")):
                # remove stray files
                remove = True
            else:
                bundle_descr = f.split(".")[0]
                if bundle_descr not in bundle_descriptors:
                    remove = True

            if remove:
                subpath = os.path.join(dir, f)
                log.info("%s %s" % (verb, subpath))
                if not dry_run:
                    self._storage.delete(subpath)

        ### 3. clean objects

        all_objects = set()
        for bundle_desc in bundle_descriptors:
            manifest = self.manifest_for_bundle_descriptor(bundle_desc)
            for f, meta in manifest.files.iteritems():
                all_objects.add(meta['sha'])

        dir = self._ph.objects_dir
        for path in self._storage.list(dir):

            basename = os.path.basename(path)
            obj = os.path.splitext(basename)[0]

            if obj not in all_objects:
                subpath = os.path.join(dir, path)
                log.info("%s %s" % (verb, subpath))
                if not dry_run:
                    self._storage.delete(subpath)
