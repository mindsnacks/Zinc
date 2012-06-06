import logging
import optparse
import os
import json
from shutil import copyfile
from os.path import join as pjoin
import tarfile

import errno
from utils import sha1_for_path, canonical_path, makedirs, mygzip
ZINC_FORMAT = "1"
ZINC_REPO_INDEX = "index.json"
ZINC_REPO_CONFIG = "config.json"

IGNORE = ['.DS_Store']

logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s')



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

### ZincOperation ############################################################

class ZincOperation(object):

    def commit():
        pass

### ZincIndex ################################################################

class ZincIndex(object):

    def __init__(self, id=None):
        self.format = ZINC_FORMAT
        self._bundles = {}
        self.distributions = {}
        self.id = id

    def get_bundles(self):
        return self._bundles

    def set_bundles(self, bundles):
        """This ensures that the version of the bundle are always sorted"""
        sorted_bundles = {}
        for (k,v) in bundles.items():
            sorted_bundles[k] = sorted(v)
        self._bundles = sorted_bundles

    def del_bundles(self):
        del self._bundles

    bundles = property(get_bundles, set_bundles, del_bundles, "Bundles property")

    def to_json(self):
        return {
                'id' : self.id,
                'bundles' : self.bundles,
                'distributions' : self.distributions,
                'format' : self.format,
                }

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

    def add_version_for_bundle(self, bundle_name, version):
        versions = self._bundles.get(bundle_name) or []
        if version not in versions:
            versions.append(version)
            self._bundles[bundle_name] = sorted(versions)

    def versions_for_bundle(self, bundle_name):
        return self._bundles.get(bundle_name) or []

    def del_version_for_bundle(self, bundle_name, version):
        versions = self.versions_for_bundle(bundle_name)
        if version in versions:
            versions.remove(version)

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        if bundle_version == 'latest':
            bundle_version = self.versions_for_bundle(bundle_name)[-1]
        elif int(bundle_version) not in self.versions_for_bundle(bundle_name):
            raise ValueError("Invalid bundle version")
        distribution = self.distributions.get(distribution_name)
        if distribution is None: distribution = {}
        distribution[bundle_name] = bundle_version
        self.distributions[distribution_name] = distribution
        
def load_index(path):
    index_file = open(path, 'r')
    dict = json.load(index_file)
    index_file.close()
    index = ZincIndex()
    index.id = dict['id']
    index.format = dict['format']
    index.bundles = dict['bundles']
    index.distributions = dict['distributions']
    return index

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

    def __init__(self, bundle_id, version=1, catalog=None):
        self.bundle_id = bundle_id
        self.version = int(version)
        self.catalog = catalog
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
                'bundle' : self.bundle_id,
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
                and self.bundle_id == other.bundle_id \
                and self.files_are_equivalent(other)

def load_manifest(path):
    manifest_file = open(path, 'r')
    dict = json.load(manifest_file)
    manifest_file.close()
    bundle_id = dict['bundle']
    version = int(dict['version'])
    manifest = ZincManifest(bundle_id, version)
    manifest.files = dict['files']
    return manifest

##############################################################################

class CreateBundleVersionOperation(ZincOperation):

    def __init__(self, catalog, bundle_name, src_dir):
        self.catalog = catalog
        self.bundle_name = bundle_name
        self.src_dir =  canonical_path(src_dir)

    def _next_version_for_bundle(self, bundle_name):
        versions = self.catalog.versions_for_bundle(bundle_name)
        if len(versions) == 0:
            return 1
        return versions[-1] + 1

    def run(self):
        version = self._next_version_for_bundle(self.bundle_name)

        # Create a new manifest outside of the catalog
        bundle_id = self.catalog.bundle_id_for_name(self.bundle_name)
        new_manifest = ZincManifest(bundle_id, version)

        # Process all the paths and add them to the manifest
        for root, dirs, files in os.walk(self.src_dir):
            for f in files:
                if f in IGNORE: continue # TODO: real ignore
                full_path = pjoin(root, f)
                rel_dir = root[len(self.src_dir)+1:]
                rel_path = pjoin(rel_dir, f)
                sha = sha1_for_path(full_path)
                new_manifest.add_file(rel_path, sha)
       
        existing_manifest = self.catalog.manifest_for_bundle(self.bundle_name)
        if existing_manifest is None or not new_manifest.files_are_equivalent(existing_manifest):

            tar_file_name = self.catalog._path_for_archive_for_bundle_version(self.bundle_name, version)
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

            self.catalog.index.add_version_for_bundle(self.bundle_name, version)
            self.catalog._write_manifest(new_manifest)
            self.catalog.save()
            return new_manifest

        return existing_manifest

#### ZincBundle ###############################################################
#
##class ZincBundle(object):
##
##    def __init__(self, manifest):
##        self.manifest = manifest 
#
### ???
##def ZincMetaBundle(object):
##    # bundle name
##    # version
##    pass

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

    config_path = pjoin(path, ZINC_REPO_CONFIG)
    ZincConfig().write(config_path)

    index_path = pjoin(path, ZINC_REPO_INDEX)
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
        index_path = pjoin(self.path, ZINC_REPO_INDEX)
        self.index = load_index(index_path)
        if self.index.format != ZINC_FORMAT:
            raise Exception("Incompatible format %s" % (self.index.format))

    def _read_config_file(self):
        config_path = pjoin(self.path, ZINC_REPO_CONFIG)
        self.config = load_config(config_path)

    def _write_index_file(self):
        index_path = pjoin(self.path, ZINC_REPO_INDEX)
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
        bundle_name = manifest.bundle_id.split('.')[-1]
        return self._path_for_manifest_for_bundle_version(bundle_name,
                manifest.version)

    def manifest_for_bundle(self, bundle_name, version=None):
        all_versions = self.index.versions_for_bundle(bundle_name)
        if version is None and len(all_versions) > 0:
            version = all_versions[-1]
        elif version not in all_versions:
            return None # throw exception?
        manifest_path = self._path_for_manifest_for_bundle_version(bundle_name, version)
        return load_manifest(manifest_path)

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

    def clean(self):
        pass

    def verify(self):
        if not self._loaded:
            raise Exception("not loaded")
            # TODO: better exception
            # TODO: wrap in decorator?

        for (bundle_name, versions) in self.index.bundles.iteritems():
            for version in versions:
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

        bundle_id = self.bundle_id_for_name(bundle_name)
        manifest = ZincManifest(bundle_id, version, self)
        self._write_manifest(manifest)
        self.index.add_version_for_bundle(bundle_name, version)
        return manifest

    def bundle_id_for_name(self, bundle_name):
        return self.index.id + "." + bundle_name

    def versions_for_bundle(self, bundle_name):
        return self.index.versions_for_bundle(bundle_name)

    def bundle_names(self):
        return self.index.bundles.keys()

    def create_bundle_version(self, bundle_name, src_dir):
        op = CreateBundleVersionOperation(self, bundle_name, src_dir)
        return op.run()

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        self.index.update_distribution(distribution_name, bundle_name, bundle_version)
        self.save()

    def save(self):
        #self._write_manifests()
        self._write_index_file()

### Commands #################################################################

def _cmd_verify(path):
    catalog = ZincCatalog(path)
    results = catalog.verify()
    error_count = total_count = 0
    for (file, error) in results.items():
        if error.code != ZincErrors.OK.code:
            print error.message + " : " + file
            error_count = error_count + 1
        total_count = total_count + 1
    print "Verified %d files, %d errors" % (total_count, error_count)

### Main #####################################################################

def main():

    #commands = {
    #        "catalog" : ("create", "verify"),
    #        #"bundle": ("create", "commit"),
    #        }

    commands = ("catalog:create",
            "catalog:verify",
            "bundle:update",
            "distro:update",
            )

    usage = "usage: %prog <command> [options]"
    parser = optparse.OptionParser(usage)
    parser.add_option("-i", "--input", dest="input_path",
                    action="store",
                    help="Input (source) path")
    parser.add_option("-o", "--output", dest="output_path",
                    action="store",
                    help="Output (destination) path")
    parser.add_option("-u", "--url", dest="url_root",
                    action="store",
                    help="Root deployment URL")

#    parser.add_option("-c", "--config", dest="config",
#                    action="store",
#                    default=default_config,
#                    help="Config file")
#    parser.add_option("-d", "--deploy-config", dest="deploy_config",
#                    action="store",
#                    default="live",
#                    type="choice",
#                    choices=("dev", "live"),
#                    help="Choose 'dev' or 'live' deployment. Default: 'live'")
#    parser.add_option("-D", "--delete", dest="delete",
#                    action="store_true",
#                    default=False,
#                    help="Delete existing local files first.")
#
    options, args = parser.parse_args()

    if len(args) == 0 or args[0] not in commands:
        parser.print_usage()
        exit(1)

    # TODO: better command parsing
    command = args[0]
    if command == "catalog:verify":
        if len(args) < 2:
            parser.print_usage()
            exit(1)
        path = args[1]
        _cmd_verify(path)
        exit(0)
    elif command == "catalog:create": 
        if len(args) < 3:
            parser.print_usage()
            exit(1)
        id = args[1]
        path = args[2]
        create_catalog_at_path(path, id)
        exit(0)
    elif command == "bundle:update": 
        if len(args) < 3:
            #parser.print_usage()
            print "bundle:update <bundle name> <path>"
            exit(1)
        catalog = ZincCatalog(".")
        bundle_name = args[1]
        path = args[2]
        manifest = catalog.create_bundle_version(bundle_name, path)
        print "Updated %s v%d" % (manifest.bundle_id, manifest.version)
        exit(0)
    elif command == "distro:update": 
        if len(args) < 4:
            #parser.print_usage()
            print "distro:update <distro name> <bundle name> <bundle version>"
            exit(1)
        catalog = ZincCatalog(".")
        distro_name = args[1]
        bundle_name = args[2]
        bundle_version = args[3]
        catalog.update_distribution(distro_name, bundle_name, bundle_version)
        exit(0)


if __name__ == "__main__":
    main()

