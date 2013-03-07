import logging
from urlparse import urlparse
from functools import wraps
import tempfile

from zinc.models import ZincIndex, ZincManifest, ZincCatalogConfig
from zinc.catalog import ZincAbstractCatalog, ZincCatalogPathHelper

from zinc.defaults import defaults
from zinc.utils import *
from zinc.helpers import *

VALID_FORMATS = ('raw', 'gz') # TODO: relocate this

# tmp?
import tarfile

################# TEMP ######################

def build_archive(catalog_coordinator, manifest, flavor=None):

    archive_filename = catalog_coordinator.path_helper.archive_name(
            manifest.bundle_name, manifest.version, flavor=flavor)
    archive_path = os.path.join(
            tempfile.mkdtemp(), archive_filename)
   
    files = manifest.get_all_files(flavor=flavor)
    
    with tarfile.open(archive_path, 'w') as tar:
        for f in files:
            format, format_info = manifest.get_format_info_for_file(f)
            sha = manifest.sha_for_file(f)
            ext = file_extension_for_format(format)
           
            fileobj = catalog_coordinator.get_file(sha, ext=ext)

            tarinfo = tar.tarinfo()
            tarinfo.name = filename_with_ext(sha, ext)
            tarinfo.size = format_info['size']
            
            tar.addfile(tarinfo, fileobj)

    return archive_path


################################################################################

class ZincCatalog(ZincAbstractCatalog):

    def __init__(self, coordinator=None, storage=None, 
            path_helper=None, **kwargs):
        assert coordinator
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
        return self._coordinator.url

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
        logging.warn('reimplement config loading')
        self.config = ZincCatalogConfig()
        #config_path = pjoin(self.path, defaults['catalog_config_name'])
        #self.config = load_config(config_path)

    def _lock(func):
        @wraps(func)
        def with_lock(self, *args, **kwargs):
            lock = self._coordinator.get_index_lock()
            lock.acquire(timeout=self.lock_timeout)
            output = func(*args, **kwargs)
            self._save()
            lock.release()
            return output
        return with_lock

    def _write_index_file(self):
        self.write_index(self.index)

    def _write_manifest(self, manifest):
        self.write_manifest(manifest, True)


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
        
        # TODO: fix private ref to _bundle_info_by_name
        for (bundle_name, bundle_info) in self.index._bundle_info_by_name.iteritems():
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
        if version in self.index.versions_for_bundle(bundle_name):
            raise ValueError("Bundle already exists")
            return None

        manifest = ZincManifest(self.index.id, bundle_name, version)
        self._write_manifest(manifest)
        self.index.add_version_for_bundle(bundle_name, version)
        return manifest


    def delete_distribution(self, distribution_name, bundle_name):
        self.index.delete_distribution(distribution_name, bundle_name)
        self._save()

    def _save(self):
        self._write_index_file()

    ### helpers

    def puts(self, subpath, bytes, raw=True, gzip=True):
        if raw:
            self._storage.puts(subpath, bytes)
        if gzip:
            self._storage.puts(subpath+'.gz', gzip_bytes(bytes))

    def _read_index(self):
        subpath = self._ph.path_for_index()
        bytes = self.get_path(subpath)
        return ZincIndex.from_bytes(bytes)

    def write_index(self, index, raw=True, gzip=True):
        subpath = self._ph.path_for_index()
        bytes = index.to_bytes()
        self.puts(subpath, bytes, raw=raw, gzip=gzip)

    def write_manifest(self, manifest, raw=True, gzip=True):
        subpath = self._ph.path_for_manifest(manifest)
        bytes = manifest.to_bytes()
        self.puts(subpath, bytes, raw=raw, gzip=gzip)

    def read_manifest(self, bundle_name, version):
        subpath = self._ph.path_for_manifest_for_bundle_version(
                bundle_name, version)
        bytes = self.get_path(subpath)
        return ZincManifest.from_bytes(bytes)

    def write_file(self, sha, src_path, format=None):
        format = format or 'raw' # default to 'raw'
        if format not in VALID_FORMATS:
            raise Exception("Invalid format '%s'." % (format))
        ext = format if format != 'raw' else None
        subpath = self._ph.path_for_file_with_sha(sha, ext)
        with open(src_path, 'r') as src_file:
            self._storage.put(subpath, src_file)
        return subpath

    def get_file_info(self, sha, preferred_formats=None):
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

    def get_file(self, sha, ext=None):
        subpath = self._ph.path_for_file_with_sha(sha, ext=ext)
        return self._storage.get(subpath)

    def get_path(self, rel_path):
        return self._storage.get(rel_path).read()

    def write_archive(self, bundle_name, version, src_path, flavor=None):
        subpath = self._ph.path_for_archive_for_bundle_version(
                bundle_name, version, flavor=flavor)
        with open(src_path, 'r') as src_file:
            self._storage.put(subpath, src_file)
        return subpath

    ### "Public" Methods

    def get_index(self):
        return self.index.clone(mutable=False)

    def get_manifest(self, bundle_name, version):
        return self.read_manifest(bundle_name, version)

    @_lock
    def update_bundle(self, bundle_name, filelist, 
            skip_master_archive=False, force=False):

        assert bundle_name
        assert filelist

        ## Check if it matches the newest version
        ## TODO: optionally check it if matches any existing versions?
   
        if not force:
            existing_manifest = self.manifest_for_bundle(bundle_name)
            if existing_manifest is not None and existing_manifest.files == filelist:
                logging.info("Found existing version with same contents.")
                return existing_manifest
    
        ## verify all files in the filelist exist in the repo
        
        missing_shas = list()
    
        for path in filelist.keys():
            sha = filelist.sha_for_file(path)
            file_info = self.get_file_info(sha)
            if file_info is None:
                missing_shas.append(sha)
    
        if len(missing_shas) > 0:
            # TODO: better error
            raise Exception("Missing shas: %s" % (missing_shas))
    
        ## build manifest
        
        version = self.index.next_version_for_bundle(bundle_name)
        new_manifest = ZincManifest(self.id, bundle_name, version)
        new_manifest.files = filelist
    
        ## Handle archives
    
        should_create_archives = len(filelist) > 1
        if should_create_archives:
            
            archive_flavors = list()
    
            # should create master archive?
            if len(new_manifest.flavors) == 0 or not skip_master_archive:
                # None is the appropriate flavor for the master archive
                archive_flavors.append(None) 
    
            # should create archives for flavors?
            if new_manifest.flavors is not None:
                archive_flavors.extend(new_manifest.flavors)
    
            for flavor in archive_flavors:
                tmp_tar_path = build_archive(
                        self, new_manifest, flavor=flavor)
                self.write_archive(
                        bundle_name, new_manifest.version, 
                        tmp_tar_path, flavor=flavor)
               
                # TODO: remove remove?
                os.remove(tmp_tar_path)
    
        ## write manifest
    
        self._write_manifest(new_manifest)
        
        ## update catalog index
        
        self.index.add_version_for_bundle(bundle_name, version)
        self._save()

        return new_manifest

    def import_path(self, src_path):

        sha = sha1_for_path(src_path)

        file_info = self.get_file_info(sha)
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
                format = 'gz'
            else:
                final_src_path = src_path
                final_src_size = src_size
                format = 'raw'
    
            imported_path = self.write_file(
                    sha, final_src_path, format=format)

        file_info = {
                'sha' :  sha,
                'size' : final_src_size,
                'format' : format
                }
        logging.info("Imported %s --> %s" % (src_path, file_info))
        logging.debug("Imported path: %s" % imported_path)
        return  file_info

    @_lock
    def delete_bundle_version(self, bundle_name, version):
        self.index.delete_bundle_version(bundle_name, version)
        self._save()

    @_lock
    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        self.index.update_distribution(distribution_name, bundle_name, bundle_version)
        self._save()


################################################################################

class ZincService(object):

    pass

    #@property
    #def coordinator(self):
    #    return self._coordinator

    #@property
    #def storage(self):
    #    return self._storage
