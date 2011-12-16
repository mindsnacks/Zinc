import logging
import optparse
import os
import errno
import hashlib
import string
import json
from shutil import copyfile
from os.path import join as pjoin

ZINC_FORMAT = "1"

logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s')

### Utils ####################################################################

def sha1_for_path(path):
    """Returns the SHA1 hash as a string for the given path"""
    sha1 = hashlib.sha1()
    f = open(path, 'rb')
    try:
        sha1.update(f.read())
    finally:
        f.close()
    return sha1.hexdigest()

def canonical_path(path):
    path = os.path.expanduser(path)
    path = os.path.normpath(path)
    path = os.path.realpath(path)
    return path

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

    def __init__(self):
        self.format = ZINC_FORMAT
        self._bundles = {}
        self.distributions = {}

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
                'bundles' : self.bundles,
                'distributions' : self.distributions,
                'format' : self.format,
                }

    def write(self, path):
        index_file = open(path, 'w')
        dict = self.to_json()
        index_file.write(json.dumps(dict, indent=2, sort_keys=True))
        index_file.close()

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
        if bundle_version not in self.versions_for_bundle(bundle_name):
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
    index.format = dict['format']
    index.bundles = dict['bundles']
    index.distributions = dict['distributions']
    return index

### ZincConfig ###############################################################

class ZincConfig(object):

    def __init__(self):
        pass

    def write(self, path):
        config_file = open(path, 'w')
        config_file.write('')
        config_file.close()

### ZincManifest #############################################################

class ZincManifest(object):

    def __init__(self, bundle_name, version=1, repo=None):
        self.bundle_name = bundle_name
        self.version = int(version)
        self.repo = repo
        self.files = {}

    def add_file(self, path, sha):
        self.files[path] = sha

    def to_json(self):
        return {
                'bundle' : self.bundle_name,
                'version' : self.version,
                'files' : self.files,
                }

    def write(self, path):
        manifest_file = open(path, 'w')
        dict = self.to_json()
        manifest_file.write(json.dumps(dict, indent=2, sort_keys=True))
        manifest_file.close()

    def files_are_equivalent(self, other):
        # check that the keys are all the same
        if len(set(self.files.keys()) - set(other.files.keys())) != 0:
            return False
        if len(set(other.files.keys()) - set(self.files.keys())) != 0:
            return False
        # if the keys are all the same, check the values
        for (file, sha) in self.files.items():
            other_sha = other.files.get(file)
            if other_sha is None or sha != other_sha:
                return False
        return True

    def equals(self, other):
        return self.version == other.version \
                and self.bundle_name == other.bundle_name \
                and self.files_are_equivalent(other)

def load_manifest(path):
    manifest_file = open(path, 'r')
    dict = json.load(manifest_file)
    manifest_file.close()
    bundle_name = dict['bundle']
    version = int(dict['version'])
    manifest = ZincManifest(bundle_name, version)
    manifest.files = dict['files']
    return manifest

##############################################################################

class CreateBundleVersionOperation(ZincOperation):

    def __init__(self, repo, bundle_name, src_dir):
        self.repo = repo
        self.bundle_name = bundle_name
        self.src_dir =  canonical_path(src_dir)

    def _next_version_for_bundle(self, bundle_name):
        versions = self.repo.versions_for_bundle(bundle_name)
        if len(versions) == 0:
            return 1
        return versions[-1] + 1

    def run(self):
        version = self._next_version_for_bundle(self.bundle_name)

        # Create a new manifest outside of the repo
        new_manifest = ZincManifest(self.bundle_name, version)

        # Process all the paths and add them to the manifest
        for root, dirs, files in os.walk(self.src_dir):
            for f in files:
                full_path = pjoin(root, f)
                rel_dir = root[len(self.src_dir)+1:]
                rel_path = pjoin(rel_dir, f)
                sha = sha1_for_path(full_path)
                new_manifest.add_file(rel_path, sha)
       
        existing_manifest = self.repo.manifest_for_bundle(self.bundle_name)
        if existing_manifest is None or not new_manifest.files_are_equivalent(existing_manifest):
            for file in new_manifest.files.keys():
                full_path = pjoin(self.src_dir, file)
                self.repo._import_path(full_path)
            self.repo.index.add_version_for_bundle(self.bundle_name, version)
            self.repo._write_manifest(new_manifest)
            self.repo.save()
            return new_manifest
        return existing_manifest

### ZincBundle ###############################################################

class ZincBundle(object):

    def __init__(self, manifest):
        self.manifest = manifest 

# ???
def ZincMetaBundle(object):
    # bundle name
    # version
    pass

### ZincRepo #################################################################

def create_repo_at_path(path):

    path = canonical_path(path)

    zinc_dir = pjoin(path, "zinc")
    os.makedirs(zinc_dir)
        
    config_path = pjoin(zinc_dir, "config")
    ZincConfig().write(config_path)

    index_path = pjoin(path, "index.json")
    ZincIndex().write(index_path)

    # TODO: check exceptions?

    return ZincRepo(path)

class ZincRepo(object):

    def _load(self):
        self._read_index_file()
        # TODO: check format, just assume 1 for now
        #self._read_manifests()
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

    def _objects_dir(self):
        objects_path = pjoin(self.path, "objects")
        if not os.path.exists(objects_path):
            os.mkdir(objects_path)
        return objects_path

    def _manifests_dir(self):
        manifests_path = pjoin(self.path, "manifests")
        if not os.path.exists(manifests_path):
            os.mkdir(manifests_path)
        return manifests_path

    def _read_index_file(self):
        index_path = pjoin(self.path, "index.json")
        self.index = load_index(index_path)
        if self.index.format != ZINC_FORMAT:
            raise Exception("Incompatible format %s" % (self.index.format))

    def _write_index_file(self):
        index_path = pjoin(self.path, "index.json")
        self.index.write(index_path)

    def _path_for_manifest_for_bundle_version(self, bundle_name, version):
        manifest_filename = "%s-%d.json" % (bundle_name, version)
        manifest_path = pjoin(self._manifests_dir(), manifest_filename)
        return manifest_path

    def _path_for_manifest(self, manifest):
        return self._path_for_manifest_for_bundle_version(manifest.bundle_name,
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
        manifest.write(self._path_for_manifest(manifest))

    def _path_for_object_with_sha(self, sha):
        return pjoin(self._objects_dir(), sha)

    def _import_path(self, src_path):
        src_path_sha = sha1_for_path(src_path)
        dst_path = self._path_for_object_with_sha(src_path_sha)
        if not os.path.exists(dst_path):
            logging.info("Importing: %s" % src_path)
            copyfile(src_path, dst_path)
        
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
        manifest = ZincManifest(bundle_name, version, self)
        self._write_manifest(manifest)
        self.index.add_version_for_bundle(bundle_name, version)
        #if self._manifests.get(bundle_name) is None:
        #    self._manifests[bundle_name] = {} # create version dict
        #self._manifests[bundle_name][int(version)] = manifest
        return manifest

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
    repo = ZincRepo(path)
    results = repo.verify()
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
    #        "repo" : ("create", "verify"),
    #        #"bundle": ("create", "commit"),
    #        }

    commands = ("repo:create",
            "repo:verify",
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
    if command == "repo:verify":
        if len(args) < 2:
            parser.print_usage()
            exit(1)
        path = args[1]
        _cmd_verify(path)
        exit(0)
    elif command == "repo:create": 
        if len(args) < 2:
            parser.print_usage()
            exit(1)
        path = args[1]
        create_repo_at_path(path)
        exit(0)
    elif command == "bundle:update": 
        if len(args) < 3:
            #parser.print_usage()
            print "bundle:update <bundle name> <path>"
            exit(1)
        repo = ZincRepo(".")
        bundle_name = args[1]
        path = args[2]
        manifest = repo.create_bundle_version(bundle_name, path)
        print "Updated %s v%d" % (manifest.bundle_name, manifest.version)
        exit(0)
    elif command == "distro:update": 
        if len(args) < 4:
            #parser.print_usage()
            print "distro:update <distro name> <bundle name> <bundle version>"
            exit(1)
        repo = ZincRepo(".")
        distro_name = args[1]
        bundle_name = args[2]
        bundle_version = int(args[3])
        repo.update_distribution(distro_name, bundle_name, bundle_version)
        exit(0)


if __name__ == "__main__":
    main()





























def old_crap():
    src = os.path.realpath(args[0])
    dst = os.path.realpath(args[1])

    # do 1st time create:
    #  - copy over all files with hash name
    #  - generate json manifest file

    # 2nd + run
    #  - verify entire zinc instance
    #  - read existing manifest for the desired version (if exists)
    #  - if manifest exists, check against new file list
    #  - if any files are missing, throw error

    create_dir_if_needed(dst)

    os.chdir(src)
    file_list = dict()
    
    version_major = '1'
    version_minor = '2'

    for root, dirs, files in os.walk("."):
        cur_dst_dir = pjoin(dst, "objects", root)
        create_dir_if_needed(cur_dst_dir)
        for f in files:
            root = string.lstrip(root, "./")
            full_src_path = pjoin(root, f)
            sha = sha1_for_path(full_src_path)

            (src_name, src_ext) = os.path.splitext(f)
            dst_name = src_name + "+" + sha + src_ext
            full_dst_path = pjoin(cur_dst_dir, dst_name)
            copyfile(full_src_path, full_dst_path)

            #os.chdir(cur_dst_dir)
            #os.system("zsyncmake %s" % (dst_name))
            #os.chdir(src)

            file_list[full_src_path] = sha

    version_path = pjoin(dst, "versions", version_major)
    create_dir_if_needed(version_path)

    manifest_name = "manifest"
    manifest = {'version' : "%s.%s"  %( version_major, version_minor),
            'files' : file_list}
    manifest_path = pjoin(version_path, manifest_name + ".json")
    manifest_file = open(manifest_path, "w")
    manifest_file.write(json.dumps(manifest, indent=2, sort_keys=True))
    manifest_file.close()

    #sha = sha1_for_path(manifest_path)
    #sha_path = pjoin(version_path, manifest_name + ".sha")
    #sha_file = open(sha_path, "w")
    #sha_file.write(sha)
    #sha_file.close()

    # write index file
    index = {
            'zinc_fileormat' :  ZINC_FORMAT,
            }
    index_path = pjoin(dst, "index.json")
    index_file = open(index_path, "w")
    index_file.write(json.dumps(index, indent=2, sort_keys=True))
    index_file.close()

