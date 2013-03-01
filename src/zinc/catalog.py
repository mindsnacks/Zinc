import json
import logging
from urlparse import urlparse, urljoin
from os.path import join as pjoin

from zinc.utils import *
from zinc.defaults import defaults
from zinc.models import ZincIndex, ZincManifest

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

    def path_for_index(self):
        return defaults['catalog_index_name']

    def path_for_manifest_for_bundle_version(self, bundle_name, version):
        manifest_filename = "%s-%d.json" % (bundle_name, version)
        manifest_path = pjoin(self.manifests_dir, manifest_filename)
        return manifest_path

    def path_for_manifest(self, manifest):
        return self.path_for_manifest_for_bundle_version(
                manifest.bundle_name, manifest.version)

    def path_for_file_with_sha(self, sha, ext=None):
        subdir = pjoin(self.objects_dir, sha[0:2], sha[2:4])
        file = sha
        if ext is not None:
            file = file + '.' + ext
        return pjoin(subdir, file)

    def path_for_archive_for_bundle_version(
            self, bundle_name, version, flavor=None):
        if flavor is None:
            archive_filename = "%s-%d.tar" % (bundle_name, version)
        else:
            archive_filename = "%s-%d~%s.tar" % (bundle_name, version, flavor)
        archive_path = pjoin(self.archives_dir, archive_filename)
        return archive_path


### 

class CatalogCoordinator(object):

    def __init__(self, url=None, storage=None, path_helper=None):
        assert url is not None
        assert self.validate_url(url)
        assert storage is not None # TODO: replace

        self._url = url
        self._storage = storage
        self._ph = path_helper or ZincCatalogPathHelper()

        self._after_init()

    def _after_init(self):
        pass

    @property
    def url(self):
        return self._url

    def validate_url(self, url):
        raise Exception("Must be overridden by subclasses.")

    def read_index(self):
        path = self._ph.path_for_index()
        bytes = self._storage.get(path)
        return ZincIndex.from_bytes(bytes)

    def write_index(self, index):
        index.write(self._index_path(), True)

    def write_manifest(self, manifest, raw=True, gzip=True):
        subpath = self._ph.path_for_manifest(manifest)
        bytes = manifest.to_bytes()
        if raw:
            self._storage.put(subpath, bytes)
        if gzip:
            self._storage.put(subpath+'.gz', gzip_bytes(bytes))

    def get_path(self, rel_path):
        return self._storage.get(rel_path)

class StorageBackend(object):

    def __init__(self, url=None, ):
        assert url is not None
        self._url = url

    @property
    def url(self):
        return self._url
    
    def read(self, subpath):
        raise Exception("Must be overridden by subclasses.")


### ZincCatalogConfig ###############################################################

class ZincCatalogConfig(object):

    def __init__(self):
        self.gzip_threshhold = 0.85

    def to_json(self):
        d = {}
        if self.gzip_threshhold is not None:
            d['gzip_threshhold'] = self.gzip_threshhold
        return d
   
    def write(self, path):
        config_file = open(path, 'w')
        dict = self.to_json()
        config_file.write(json.dumps(dict))
        config_file.close()

def load_config(path):
    config_file = open(path, 'r')
    dict = json.load(config_file)
    config_file.close()
    config = ZincCatalogConfig()
    if dict.get('gzip_threshhold'):
        config.gzip_threshhold = dict.get('gzip_threshhold')
    return config 


### ZincCatalog ################################################################

class ZincCatalog(object):

    def _reload(self):
        #self._read_index_file()
        ## TODO: check format, just assume 1 for now
        
        self.index = self._coordinator.read_index()
        if self.index.format != defaults['zinc_format']:
            raise Exception("Incompatible format %s" % (self.index.format))

        self._read_config_file()
        #self._loaded = True

    #def __init__(self, path):
    #    self._loaded = False
    #    self.path = canonical_path(path)
    #    self._manifests = {}
    #    self._load()

    def __init__(self, coordinator=None):
        assert coordinator

        self._coordinator = coordinator
        self._manifests = {}
        self._ph = ZincCatalogPathHelper()
        self._reload()

    @property
    def url(self):
        return self._coordinator.url

    @property
    def path(self):
        return urlparse(self.url).path

    @property
    def id(self):
        return self.index.id

    def format(self):
        return self.index.format

    #def is_loaded(self):
    #    return self._loaded

    def _read_config_file(self):
        logging.warn('reimplement config loading')
        self.config = ZincCatalogConfig()
        #config_path = pjoin(self.path, defaults['catalog_config_name'])
        #self.config = load_config(config_path)

    def _write_index_file(self):
        index_path = pjoin(self.path, defaults['catalog_index_name'])
        self.index.write(index_path, True)

    def manifest_for_bundle(self, bundle_name, version=None):
        all_versions = self.index.versions_for_bundle(bundle_name)
        if version is None and len(all_versions) > 0:
            version = all_versions[-1]
        elif version not in all_versions:
            return None # throw exception?
        manifest_path = self._ph.path_for_manifest_for_bundle_version(
                bundle_name, version)
        data = self._coordinator.get_path(manifest_path)
        return ZincManifest.from_bytes(data)

    def manifest_for_bundle_descriptor(self, bundle_descriptor):
        return self.manifest_for_bundle(
            bundle_id_from_bundle_descriptor(bundle_descriptor),
            bundle_version_from_bundle_descriptor(bundle_descriptor))
            
    def _write_manifest(self, manifest):
        self._coordinator.write_manifest(manifest, True)

    def _import_path(self, src_path):
        src_path_sha = sha1_for_path(src_path)

        # TODO: fix
        dst_path = os.path.join(self.path, self._ph.path_for_file_with_sha(src_path_sha))
        dst_path_gz = dst_path+'.gz'

        makedirs(os.path.dirname(dst_path))

        # TODO: this is lame
        if os.path.exists(dst_path):
            return (dst_path, os.path.getsize(dst_path))
        elif os.path.exists(dst_path_gz):
            return (dst_path_gz, os.path.getsize(dst_path_gz))

        # gzip the file first, and see if it passes the compression
        # threshhold

        makedirs(os.path.dirname(dst_path))
        gzip_path(src_path, dst_path_gz)
        src_size = os.path.getsize(src_path)
        dst_gz_size = os.path.getsize(dst_path_gz)
        if src_size > 0 and float(dst_gz_size) / src_size <= self.config.gzip_threshhold:
            final_dst_path = dst_path_gz
            final_dst_size = dst_gz_size
        else:
            final_dst_path = dst_path
            final_dst_size = src_size
            copyfile(src_path, dst_path)
            os.remove(dst_path_gz)

        logging.info("Imported %s --> %s" % (src_path, final_dst_path))
        return (final_dst_path, final_dst_size)
        
    def bundle_descriptors(self):
        bundle_descriptors = []
        for bundle_name in self.bundle_names():
            for version in self.versions_for_bundle(bundle_name):
                bundle_descriptors.append("%s-%d" % (bundle_name, version))
                manifest = self.manifest_for_bundle(bundle_name, version)
                for flavor in manifest.flavors:
                    bundle_descriptors.append("%s-%d~%s" % 
                            (bundle_name, version, flavor))
        return bundle_descriptors

#    def clean(self, dry_run=False):
#        bundle_descriptors = self.bundle_descriptors()
#        verb = 'Would remove' if dry_run else 'Removing'
#
#        ### 1. scan manifests for ones that aren't in index
#        for root, dirs, files in os.walk(self._manifests_url()):
#            for f in files:
#                remove = False
#                if not (f.endswith(".json") or f.endswith(".json.gz")):
#                    # remove stray files
#                    remove = True
#                else:
#                    bundle_descr = f.split(".")[0]
#                    if bundle_descr not in bundle_descriptors:
#                        remove = True
#                if remove:
#                    logging.info("%s %s" % (verb, pjoin(root, f)))
#                    if not dry_run: os.remove(pjoin(root, f))
#
#        ### 2. scan archives for ones that aren't in index
#        for root, dirs, files in os.walk(self._archives_dir()):
#            for f in files:
#                remove = False
#                if not (f.endswith(".tar")):
#                    # remove stray files
#                    remove = True
#                else:
#                    bundle_descr = f.split(".")[0]
#                    if bundle_descr not in bundle_descriptors:
#                        remove = True
#                if remove:
#                    logging.info("%s %s" % (verb, pjoin(root, f)))
#                    if not dry_run: os.remove(pjoin(root, f))
#
#        ### 3. clean objects
#        all_objects = set()
#        for bundle_desc in bundle_descriptors:
#            manifest = self.manifest_for_bundle_descriptor(bundle_desc)
#            for f, meta in manifest.files.iteritems():
#                all_objects.add(meta['sha'])
#        for root, dirs, files in os.walk(self._files_dir()):
#            for f in files:
#                basename = os.path.splitext(f)[0]
#                if basename not in all_objects:
#                    logging.info("%s %s" % (verb, pjoin(root, f)))
#                    if not dry_run: os.remove(pjoin(root, f))
#
    def verify(self):
#        if not self._loaded:
#            raise Exception("not loaded")
#            # TODO: better exception
#            # TODO: wrap in decorator?
#
        for (bundle_name, bundle_info) in self.index.bundle_info_by_name.iteritems():
            for version in bundle_info['versions']:
                manifest = self.manifest_for_bundle(bundle_name, version)
                if manifest is None:
                    raise Exception("manifest not found: %s-%d" % (bundle_name,
                        version))
                #for (file, sha) in manifest.files.iteritems():
                #    print file, sha

        results_by_file = dict()
        #for version, manifest in self.manifests.items():
        #    files = manifest.get("files")
        #    for file, sha in files.items():
        #        full_path = pjoin(self.path, self.path_for_file(file, version))
        #        logging.debug("verifying %s" % full_path)
        #        if not os.path.exists(full_path):
        #            results_by_file[file] = ZincErrors.DOES_NOT_EXIST
        #        elif sha1_for_path(full_path) != sha:
        #            results_by_file[file] = ZincErrors.INCORRECT_SHA
        #        else:
        #            # everything is ok alarm
        #            results_by_file[file] = ZincErrors.OK
        return results_by_file

    def _add_manifest(self, bundle_name, version=1):
        if version in self.versions_for_bundle(bundle_name):
            raise ValueError("Bundle already exists")
            return None

        manifest = ZincManifest(self.index.id, bundle_name, version)
        self._write_manifest(manifest)
        self.index.add_version_for_bundle(bundle_name, version)
        return manifest

    def versions_for_bundle(self, bundle_name):
        return self.index.versions_for_bundle(bundle_name)

    def bundle_names(self):
        return self.index.bundle_info_by_name.keys()

    def create_bundle_version(self, bundle_name, src_dir, 
            flavor_spec=None, force=False, skip_master_archive=False):

        from zinc.tasks.bundle_update import ZincBundleUpdateTask

        task = ZincBundleUpdateTask()
        task.catalog = self
        task.bundle_name = bundle_name
        task.src_dir = src_dir
        task.flavor_spec = flavor_spec
        task.skip_master_archive = skip_master_archive
        return task.run()

    def delete_bundle_version(self, bundle_name, version):
        self.index.delete_bundle_version(bundle_name, version)
        self.save()

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        self.index.update_distribution(distribution_name, bundle_name, bundle_version)
        self.save()

    def delete_distribution(self, distribution_name, bundle_name):
        self.index.delete_distribution(distribution_name, bundle_name)
        self.save()

    def save(self):
        #self._write_manifests()
        self._write_index_file()

    def path_in_catalog(self, path):
        return os.path.join(self.path, path)

#######################################


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

def catalog_connect(catalog_ref):
    urlcomps = urlparse(catalog_ref)
    if urlcomps.scheme in ('http', 'https'):
        _catalog_connection_get_http(catalog_ref)           
    elif urlcomps.scheme in ('file', ''):
        if urlcomps.scheme == '':
            # assume it's a path and convert a file URL
            url = 'file://%s' % (canonical_path(catalog_ref))
        else:
            url = catalog_ref

        from zinc.coordinators.filesystem import FilesystemCatalogCoordinator
        from zinc.storages.filesystem import FilesystemStorageBackend
        storage = FilesystemStorageBackend(url=url)
        coord = FilesystemCatalogCoordinator(url=url, storage=storage)
        return ZincCatalog(coordinator=coord)


# TODO: revamp this:
def create_catalog_at_path(path, id):

    path = canonical_path(path)
    try:
        makedirs(path)
    except OSError, e:
        if e.errno == 17:
            pass # directory already exists
        else:
            raise e

    config_path = pjoin(path, defaults['catalog_config_name'])
    ZincCatalogConfig().write(config_path)

    index_path = pjoin(path, defaults['catalog_index_name'])
    ZincIndex(id).write(index_path, True)

    # TODO: check exceptions?

    from zinc.coordinators.filesystem import FilesystemCatalogCoordinator
    from zinc.storages.filesystem import FilesystemStorageBackend
    url = 'file://%s' % (path)
    storage = FilesystemStorageBackend(url=url)
    coord = FilesystemCatalogCoordinator(url=url, storage=storage)
    return ZincCatalog(coordinator=coord)


