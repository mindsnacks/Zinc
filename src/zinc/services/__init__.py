from urlparse import urlparse
from functools import wraps
import tempfile
import logging

from zinc.models import ZincIndex, ZincManifest, ZincCatalogConfig
from zinc.catalog import ZincAbstractCatalog, ZincCatalogPathHelper
from zinc.formats import Formats

from zinc.defaults import defaults
from zinc.utils import *
from zinc.helpers import *

log = logging.getLogger(__name__)

################################################################################

class ZincCatalog(ZincAbstractCatalog):

    def __init__(self, storage=None, coordinator=None, path_helper=None,
                 **kwargs):
        assert storage

        super(ZincCatalog, self).__init__(**kwargs)

        self._coordinator = coordinator
        self._storage = storage

        self._ph = path_helper or ZincCatalogPathHelper()
        self._manifests = {}
        self._reload()

        self.lock_timeout = defaults['catalog_lock_timeout']

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

        ## TODO: check format, just assume 1 for now
        self.index = self._read_index()
        if self.index.format != defaults['zinc_format']:
            raise Exception("Incompatible format %s" % (self.index.format))

        self._read_config_file()

    def _read_config_file(self):
        #log.warn('reimplement config loading')
        self.config = ZincCatalogConfig()
        #config_path = pjoin(self.path, defaults['catalog_config_name'])
        #self.config = load_config(config_path)

    def _lock_index(func):
        @wraps(func)
        def with_lock_index(self, *args, **kwargs):

            assert self._coordinator

            #lock = self._coordinator.get_index_lock_index()
            #lock.acquire(timeout=self.lock_timeout)
            #output = func(*args, **kwargs)
            #lock.release()

            with self._coordinator.get_index_lock(domain=self.id):
                self._reload()
                output = func(self, *args, **kwargs)
                self.save()

            return output
        return with_lock_index

    def save(self):
        self._write_index(self.index)

    ### I/O Helpers ###

    def _read(self, rel_path):
        f = self._storage.get(rel_path)
        return f.read() if f is not None else None

    def _write(self, subpath, bytes, raw=True, gzip=True, max_age=None):
        if raw:
            self._storage.puts(subpath, bytes, max_age=max_age)
        if gzip:
            self._storage.puts(subpath + '.gz', gzip_bytes(bytes), max_age=max_age)

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
        subpath = self._ph.path_for_manifest_for_bundle_version(
                bundle_name, version)
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
                        'sha' : sha,
                        'size' : meta['size'],
                        'format' : format
                        }
        return None

    def _read_file(self, sha, ext=None):
        subpath = self._ph.path_for_file_with_sha(sha, ext=ext)
        return self._storage.get(subpath)

    def _write_file(self, sha, src_path, format=None):
        format = format or Formats.RAW # default to RAW
        if format not in defaults['catalog_valid_formats']:
            raise Exception("Invalid format '%s'." % (format))
        ext = format if format != Formats.RAW else None
        subpath = self._ph.path_for_file_with_sha(sha, ext)
        with open(src_path, 'r') as src_file:
            self._storage.put(subpath, src_file)
        return subpath

    def _get_archive_info(self, bundle_name, version, flavor=None):
        subpath = self._ph.path_for_archive_for_bundle_version(bundle_name,
                version, flavor=flavor)
        meta = self._storage.get_meta(subpath)
        return meta

    def _write_archive(self, bundle_name, version, src_path, flavor=None):
        subpath = self._ph.path_for_archive_for_bundle_version(
                bundle_name, version, flavor=flavor)
        with open(src_path, 'r') as src_file:
            self._storage.put(subpath, src_file)
        return subpath

    def _read_archive(self, bundle_name, version, flavor=None):
        subpath = self._ph.path_for_archive_for_bundle_version(
                bundle_name, version, flavor=flavor)
        return self._storage.get(subpath)


    ### "Public" Methods

    def get_index(self):
        return self.index.clone(mutable=False)

    def get_manifest(self, bundle_name, version):
        return self._read_manifest(bundle_name, version)

    @_lock_index
    def update_bundle(self, new_manifest):

        assert new_manifest

        existing_versions = self.index.versions_for_bundle(new_manifest.bundle_name)
        if new_manifest.version in existing_versions:
            raise ValueError("Bundle version already exists.")

        next_version = self.index.next_version_for_bundle(new_manifest.bundle_name)
        if new_manifest.version > next_version:
            raise ValueError("Unexpected manifest version.")

        ## verify all files in the filelist exist in the repo

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

        ## TODO: verify archives?

        ## write manifest

        self._write_manifest(new_manifest)

        ## update catalog index

        self.index.add_version_for_bundle(new_manifest.bundle_name,
                                          new_manifest.version)

    def import_path(self, src_path):

        sha = sha1_for_path(src_path)

        file_info = self._get_file_info(sha)
        if file_info is not None:
            return file_info

        # gzip the file first, and see if it passes the compression threshhold
        # TODO: this is stupid inefficient
        with tempfile.NamedTemporaryFile() as tmp_file:
            src_path_gz = tmp_file.name
            with open(src_path) as src_file:
                tmp_file.write(gzip_bytes(src_file.read()))
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

            imported_path = self._write_file(
                    sha, final_src_path, format=format)

        file_info = {
            'sha':  sha,
            'size': final_src_size,
            'format': format
        }
        log.info("Imported %s --> %s" % (src_path, file_info))
        log.debug("Imported path: %s" % imported_path)
        return  file_info

    @_lock_index
    def _reserve_version_for_bundle(self, bundle_name):
        self.index.increment_next_version_for_bundle(bundle_name)
        return self.index.next_version_for_bundle(bundle_name)

    @_lock_index
    def delete_bundle_version(self, bundle_name, version):
        self.index.delete_bundle_version(bundle_name, version)

    @_lock_index
    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        self.index.update_distribution(distribution_name, bundle_name, bundle_version)

    @_lock_index
    def delete_distribution(self, distribution_name, bundle_name):
        self.index.delete_distribution(distribution_name, bundle_name)

    @_lock_index
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


################################################################################


class ZincServiceProvider(object):
    pass


class ZincServiceConsumer(object):

    def __init__(self, **kwargs):
        pass

    def create_catalog(self, id=None, loc=None):
        raise NotImplementedError()

    def get_catalog(self, loc=None, id=None):
        raise NotImplementedError()


class CustomServiceConsumer(ZincServiceConsumer):

    def __init__(self, coordinator=None, storage=None, **kwargs):
        assert coordinator
        assert storage

        self._coordinator = coordinator
        self._storage = storage

    def get_catalog(self, loc=None, id=None):
        cat_storage = self._storage.bind_to_catalog(id=id)
        # TODO: bind to coordinator?
        return ZincCatalog(coordinator=self._coordinator, storage=cat_storage)
