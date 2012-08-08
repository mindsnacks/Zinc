import os
from os.path import join as pjoin
import tarfile
import json
import logging
from shutil import copyfile

from .utils import sha1_for_path, canonical_path, makedirs, mygzip
from .defaults import defaults

# TODO: real ignore system
IGNORE = ['.DS_Store']

### Errors ###################################################################

class ZincError(object):
    def __init__(self, code, message):
        self.code = code
        self.message = message

# TODO: might be worst python ever
class ZincErrors(object):
    OK = ZincError(0, "OK")
    INCORRECT_SHA = ZincError(1, "SHA did not match")
    DOES_NOT_EXIST = ZincError(2, "Does not exist")

### Utils

def bundle_name_from_bundle_descriptor(bundle_descriptor):
    return bundle_descriptor[:bundle_descriptor.rfind('-')]

def bundle_version_from_bundle_descritor(bundle_descriptor):
    return int(bundle_descriptor[bundle_descriptor.rfind('-') + 1:])

### ZincOperation ############################################################

class ZincOperation(object):

    def commit():
        pass

### ZincIndex ################################################################

class ZincIndex(object):

    def __init__(self, id=None):
        self.format = defaults['zinc_format']
        self.id = id
        self.bundle_info_by_name = dict()

    def to_json(self):
       return {
				'id' : self.id,
                'bundles' : self.bundle_info_by_name,
                'format' : self.format,
                }

    @classmethod
    def from_dict(cls, json_dict):
        index = cls()
        index.id = json_dict['id']
        index.format = json_dict['format']
        index.bundle_info_by_name = json_dict['bundles']
        return index

    def write(self, path, gzip=False):
        if self.id is None:
            raise ValueError("catalog id is None") # TODO: better exception?
        index_file = open(path, 'w')
        dict = self.to_json()
        index_file.write(json.dumps(dict))
        index_file.close()
        if gzip:
            gzpath = path + '.gz'
            mygzip(path, gzpath)

    def _get_or_create_bundle_info(self, bundle_name):
        if self.bundle_info_by_name.get(bundle_name) is None:
            self.bundle_info_by_name[bundle_name] = {
                    'versions':[],
                    'distributions':{},
                    }
        return self.bundle_info_by_name.get(bundle_name)

    def add_version_for_bundle(self, bundle_name, version):
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        if version not in bundle_info['versions']:
            bundle_info['versions'].append(version)
            bundle_info['versions'] = sorted(bundle_info['versions'])

    def versions_for_bundle(self, bundle_name):
        return self._get_or_create_bundle_info(bundle_name).get('versions')

    def delete_bundle_version(self, bundle_name, bundle_version):
        assert bundle_version == int(bundle_version)
        bundle_info = self.bundle_info_by_name.get(bundle_name)
        if bundle_info is None:
            raise Exception("Unknown bundle %s" % (bundle_name))
        for distro_name, distro_version in bundle_info['distributions'].iteritems():
            if distro_version == bundle_version:
                raise Exception("bundle '%s' v%d is referenced by the distribution '%s'" 
                        % (bundle_name, bundle_version, distro_name))
        versions = bundle_info['versions']
        if bundle_version in versions:
            versions.remove(bundle_version)
        if len(versions) == 0: # remove info if no more versions
            del self.bundle_info_by_name[bundle_name]
        else:
            bundle_info['versions'] = versions

        
    def distributions_for_bundle(self, bundle_name):
        bundle_info = self.bundle_info_by_name.get(bundle_name)
        if bundle_info is None:
            raise ValueError("Unknown bundle %s" % (bundle_name))
        return bundle_info['distributions']

    def version_for_bundle(self, bundle_name, distro):
        return self.distributions_for_bundle(bundle_name).get(distro)

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        if bundle_version == 'latest':
            bundle_version = self.versions_for_bundle(bundle_name)[-1]
        elif int(bundle_version) not in self.versions_for_bundle(bundle_name):
            raise ValueError("Invalid bundle version")
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        bundle_info['distributions'][distribution_name] = bundle_version

    def delete_distribution(self, distribution_name, bundle_name):
        bundle_info = self.bundle_info_by_name.get(bundle_name)
        if bundle_name is None:
            raise ValueError("Unknown bundle %s" % (bundle_name))
        del bundle_info['distributions'][distribution_name]

def load_index(path):
    with open(path, 'r') as index_file:
        json_dict = json.load(index_file)
    return ZincIndex.from_dict(json_dict)


### ZincConfig ###############################################################

class ZincConfig(object):

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
    config = ZincConfig()
    if dict.get('gzip_threshhold'):
        config.gzip_threshhold = dict.get('gzip_threshhold')
    return config 

### ZincManifest #############################################################

class ZincManifest(object):

    def __init__(self, catalog_id, bundle_name, version=1):
        self.catalog_id = catalog_id
        self.bundle_name = bundle_name
        self.version = int(version)
        self.files = {}

    def add_file(self, path, sha):
        self.files[path] = {'sha' : sha}

    def add_format_for_file(self, path, format, size):
        props = self.files[path]
        formats = props.get('formats') or {}
        formats[format] = {'size' : size}
        props['formats'] = formats

    def to_json(self):
        return {
                'catalog' : self.catalog_id,
                'bundle' : self.bundle_name,
                'version' : self.version,
                'files' : self.files,
                }

    def write(self, path, gzip=False):
        manifest_file = open(path, 'w')
        dict = self.to_json()
        manifest_file.write(json.dumps(dict))
        manifest_file.close()
        if gzip:
            gzpath = path + '.gz'
            mygzip(path, gzpath)

    def files_are_equivalent(self, other):
        # check that the keys are all the same
        if len(set(self.files.keys()) - set(other.files.keys())) != 0:
            return False
        if len(set(other.files.keys()) - set(self.files.keys())) != 0:
            return False
        # if the keys are all the same, check the values
        for (file, props) in self.files.items():
            sha = props.get('sha')
            other_sha = other.files.get(file).get('sha')
            if other_sha is None or sha != other_sha:
                return False
        return True

    def equals(self, other):
        return self.version == other.version \
                and self.catalog_id == other.catalog_id \
                and self.bundle_name == other.bundle_name \
                and self.files_are_equivalent(other)

def load_manifest(path):
    manifest_file = open(path, 'r')
    dict = json.load(manifest_file)
    manifest_file.close()
    catalog_id = dict['catalog']
    bundle_name = dict['bundle']
    version = int(dict['version'])
    manifest = ZincManifest(catalog_id, bundle_name, version)
    manifest.files = dict['files']
    return manifest


##############################################################################

class CreateBundleVersionOperation(ZincOperation):

    def __init__(self, catalog, bundle_id, src_dir):
        self.catalog = catalog
        self.bundle_id = bundle_id
        self.src_dir =  canonical_path(src_dir)

    def _next_version_for_bundle(self, bundle_id):
        versions = self.catalog.versions_for_bundle(bundle_id)
        if len(versions) == 0:
            return 1
        return versions[-1] + 1

    def run(self):
        version = self._next_version_for_bundle(self.bundle_id)

        # Create a new manifest outside of the catalog
        new_manifest = ZincManifest(self.catalog.index.id, self.bundle_id, version)

        # Process all the paths and add them to the manifest
        for root, dirs, files in os.walk(self.src_dir):
            for f in files:
                if f in IGNORE: continue # TODO: real ignore
                full_path = pjoin(root, f)
                rel_dir = root[len(self.src_dir)+1:]
                rel_path = pjoin(rel_dir, f)
                sha = sha1_for_path(full_path)
                new_manifest.add_file(rel_path, sha)
       
        existing_manifest = self.catalog.manifest_for_bundle(self.bundle_id)
        if existing_manifest is None or not new_manifest.files_are_equivalent(existing_manifest):

            tar_file_name = self.catalog._path_for_archive_for_bundle_version(self.bundle_id, version)
            tar = tarfile.open(tar_file_name, 'w')
            print tar_file_name

            for file in new_manifest.files.keys():
                full_path = pjoin(self.src_dir, file)
                (catalog_path, size) = self.catalog._import_path(full_path)
                if catalog_path[-3:] == '.gz':
                    format = 'gz'
                else:
                    format = 'raw'
                new_manifest.add_format_for_file(file, format, size)
                tar.add(catalog_path, os.path.basename(catalog_path))

            tar.close()

            self.catalog.index.add_version_for_bundle(self.bundle_id, version)
            self.catalog._write_manifest(new_manifest)
            self.catalog.save()
            return new_manifest

        return existing_manifest


### ZincCatalog #################################################################

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
    ZincConfig().write(config_path)

    index_path = pjoin(path, defaults['catalog_index_name'])
    ZincIndex(id).write(index_path, True)

    # TODO: check exceptions?

    return ZincCatalog(path)

class ZincCatalog(object):

    def _load(self):
        self._read_index_file()
        # TODO: check format, just assume 1 for now
        self._read_config_file()
        self._loaded = True

    def __init__(self, path):
        self._loaded = False
        self.path = canonical_path(path)
        self._manifests = {}
        self._load()

    def format(self):
        return self.index.format

    def is_loaded(self):
        return self._loaded

    def _files_dir(self):
        files_path = pjoin(self.path, "objects")
        if not os.path.exists(files_path):
            makedirs(files_path)
        return files_path

    def _manifests_dir(self):
        manifests_path = pjoin(self.path, "manifests")
        if not os.path.exists(manifests_path):
            makedirs(manifests_path)
        return manifests_path

    def _archives_dir(self):
        archives_path = pjoin(self.path, "archives")
        if not os.path.exists(archives_path):
            makedirs(archives_path)
        return archives_path

    def _read_index_file(self):
        index_path = pjoin(self.path, defaults['catalog_index_name'])
        self.index = load_index(index_path)
        if self.index.format != defaults['zinc_format']:
            raise Exception("Incompatible format %s" % (self.index.format))

    def _read_config_file(self):
        config_path = pjoin(self.path, defaults['catalog_config_name'])
        self.config = load_config(config_path)

    def _write_index_file(self):
        index_path = pjoin(self.path, defaults['catalog_index_name'])
        self.index.write(index_path, True)

    def _path_for_manifest_for_bundle_version(self, bundle_name, version):
        manifest_filename = "%s-%d.json" % (bundle_name, version)
        manifest_path = pjoin(self._manifests_dir(), manifest_filename)
        return manifest_path

    def _path_for_archive_for_bundle_version(self, bundle_name, version):
        archive_filename = "%s-%d.tar" % (bundle_name, version)
        archive_path = pjoin(self._archives_dir(), archive_filename)
        return archive_path

    def _path_for_manifest(self, manifest):
        return self._path_for_manifest_for_bundle_version(
                manifest.bundle_name, manifest.version)

    def manifest_for_bundle(self, bundle_name, version=None):
        all_versions = self.index.versions_for_bundle(bundle_name)
        if version is None and len(all_versions) > 0:
            version = all_versions[-1]
        elif version not in all_versions:
            return None # throw exception?
        manifest_path = self._path_for_manifest_for_bundle_version(bundle_name, version)
        return load_manifest(manifest_path)

    def manifest_for_bundle_descriptor(self, bundle_descriptor):
        return self.manifest_for_bundle(
            bundle_name_from_bundle_descriptor(bundle_descriptor),
            bundle_version_from_bundle_descritor(bundle_descriptor))
            
    def _write_manifest(self, manifest):
        manifest.write(self._path_for_manifest(manifest), True)

    def _path_for_file_with_sha(self, src_file, sha, ext=None):
        subdir = pjoin(self._files_dir(), sha[0:2], sha[2:4])
        #ext = os.path.splitext(src_file)[1]
        #return pjoin(subdir, sha+ext)
        file = sha
        if ext is not None:
            file = file + ext
        return pjoin(subdir, file)

    def _import_path(self, src_path):
        src_path_sha = sha1_for_path(src_path)
        dst_path = self._path_for_file_with_sha(src_path, src_path_sha)
        dst_path_gz = dst_path+'.gz'

        # TODO: this is lame
        if os.path.exists(dst_path):
            return (dst_path, os.path.getsize(dst_path))
        elif os.path.exists(dst_path_gz):
            return (dst_path_gz, os.path.getsize(dst_path_gz))

        # gzip the file first, and see if it passes the compression
        # threshhold

        makedirs(os.path.dirname(dst_path))
        mygzip(src_path, dst_path_gz)
        src_size = os.path.getsize(src_path)
        dst_gz_size = os.path.getsize(dst_path_gz)
        if float(dst_gz_size) / src_size <= self.config.gzip_threshhold:
            final_dst_path = dst_path_gz
            final_dst_size = dst_gz_size
        else:
            final_dst_path = dst_path
            final_dst_size = src_size
            copyfile(src_path, dst_path)
            os.remove(dst_path_gz)

        logging.info("Imported %s --> %s" % (src_path, final_dst_path))
        return (final_dst_path, final_dst_size)
        
    def lock(self):
        pass
    
    def unlock(self):
        pass

    def bundle_descriptors(self):
        bundle_descriptors = []
        for bundle_name in self.bundle_names():
            for version in self.versions_for_bundle(bundle_name):
                bundle_descriptors.append("%s-%d" % (bundle_name, version))
        return bundle_descriptors

    def clean(self, dry_run=False):
        bundle_descriptors = self.bundle_descriptors()
        verb = 'Would remove' if dry_run else 'Removing'

        ### 1. scan manifests for ones that aren't in index
        for root, dirs, files in os.walk(self._manifests_dir()):
            for f in files:
                remove = False
                if not (f.endswith(".json") or f.endswith(".json.gz")):
                    # remove stray files
                    remove = True
                else:
                    bundle_descr = f.split(".")[0]
                    if bundle_descr not in bundle_descriptors:
                        remove = True
                if remove:
                    logging.info("%s %s" % (verb, pjoin(root, f)))
                    if not dry_run: os.remove(pjoin(root, f))

        ### 2. scan archives for ones that aren't in index
        for root, dirs, files in os.walk(self._archives_dir()):
            for f in files:
                remove = False
                if not (f.endswith(".tar")):
                    # remove stray files
                    remove = True
                else:
                    bundle_descr = f.split(".")[0]
                    if bundle_descr not in bundle_descriptors:
                        remove = True
                if remove:
                    logging.info("%s %s" % (verb, pjoin(root, f)))
                    if not dry_run: os.remove(pjoin(root, f))

        ### 3. clean objects
        all_objects = set()
        for bundle_desc in bundle_descriptors:
            manifest = self.manifest_for_bundle_descriptor(bundle_desc)
            for f, meta in manifest.files.iteritems():
                all_objects.add(meta['sha'])
        for root, dirs, files in os.walk(self._files_dir()):
            for f in files:
                basename = os.path.splitext(f)[0]
                if basename not in all_objects:
                    logging.info("%s %s" % (verb, pjoin(root, f)))
                    if not dry_run: os.remove(pjoin(root, f))

    def verify(self):
        if not self._loaded:
            raise Exception("not loaded")
            # TODO: better exception
            # TODO: wrap in decorator?

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

    def create_bundle_version(self, bundle_name, src_dir):
        op = CreateBundleVersionOperation(self, bundle_name, src_dir)
        return op.run()

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

