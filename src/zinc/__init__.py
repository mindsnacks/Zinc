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
        self.bundles = {}
        self.distributions = {}

    def to_json(self):
        return vars(self)

    def write(self, path):
        index_file = open(path, 'w')
        dict = self.to_json()
        index_file.write(json.dumps(dict, indent=2, sort_keys=True))
        index_file.close()

    def add_version_for_bundle(self, bundle_name, version):
        versions = self.bundles.get(bundle_name) or []
        if version not in versions:
            versions.append(version)
            self.bundles[bundle_name] = versions

    def versions_for_bundle(self, bundle_name):
        return self.bundles.get(bundle_name) or []

    def del_version_for_bundle(self, bundle_name, version):
        versions = self.versions_for_bundle(bundle_name)
        if version in versions:
            versions.remove(version)
        
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
        manifest = self.repo._add_manifest(self.bundle_name, version)
        os.chdir(self.src_dir)
        for root, dirs, files in os.walk(self.src_dir):
            for f in files:
                path = f
                sha = sha1_for_path(path)
                self.repo._import_path(path)
                manifest.add_file(path, sha)
        self.repo.index.add_version_for_bundle(self.bundle_name, version)
        self.repo._write_manifest(manifest)
        self.repo.save()

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

#    def _read_manifests(self):
#        self.manifests = dict()
#        versions_path = pjoin(self.path, "versions")
#        for root, dirs, files in os.walk(versions_path):
#            for f in files:
#                if f == "manifest.json":
#                    manifest_path = pjoin(root, f)
#                    manifest_file = open(manifest_path, "r")
#                    manifest_dict = json.load(manifest_file)
#                    manifest_file.close()
#                    manifest_version_major = manifest_dict.get("version").split(".")[0]
#                    self.manifests[manifest_version_major] = manifest_dict

    def _path_for_manifest_for_bundle_version(self, bundle_name, version):
        manifest_filename = "%s-%d.json" % (bundle_name, version)
        manifest_path = pjoin(self._manifests_dir(), manifest_filename)
        return manifest_path

    def _path_for_manifest(self, manifest):
        return self._path_for_manifest_for_bundle_version(manifest.bundle_name,
                manifest.version)

    def _manifest_for_bundle_version(self, bundle_name, version):
        if version not in self.index.versions_for_bundle(bundle_name):
            return None # throw exception?
        manifest_path = self._path_for_manifest_for_bundle_version(bundle_name, version)
        return load_manifest(manifest_path)

    def _write_manifest(self, manifest):
        manifest.write(self._path_for_manifest(manifest))

    #def _write_manifests(self):
    #    for versions in self._manifests.values():
    #        for manifest in versions.values():
    #            self._write_manifest(manifest)

    def _path_for_object_with_sha(self, sha):
        return pjoin(self._objects_dir(), sha)

    def _import_path(self, src_path):
        src_path_sha = sha1_for_path(src_path)
        copyfile(src_path, self._path_for_object_with_sha(src_path_sha))
        
    def lock(self):
        pass
    
    def unlock(self):
        pass

    def clean(self):
        pass

    #def path_for_file(self, file, version=None):
    #    if version == None:
    #        version = self.latest_version()

    #    manifest = self.manifests.get(version)
    #    sha = manifest.get("files").get(file)
    #    (basename, ext) = os.path.splitext(file)
    #    zinc_path = pjoin("objects", "%s+%s%s" % (basename, sha, ext))
    #    return zinc_path

    def verify(self):
        if not self._loaded:
            raise Exception("not loaded")
            # TODO: better exception
            # TODO: wrap in decorator?

        for (bundle_name, versions) in self.index.bundles.iteritems():
            for version in versions:
                manifest = self._manifest_for_bundle_version(bundle_name, version)
                if manifest is None:
                    raise Exception("manifest not found: %s-%d" % (bundle_name,
                        version))
                for (key, sha) in manifest.files.iteritems():
                    if sha1_for_path(pjoin(self._objects_dir(), sha)) != sha:
                        raise Exception("Wrong SHA")

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

    #def get_bundle(self, bundle_name):
    #    return self._manifests.get(bundle_name)

    def _add_manifest(self, bundle_name, version=1):
        manifest = self.get_bundle(bundle_name, version)
        if manifest is not None:
            raise ValueError("Bundle already exists")
            return None
        manifest = ZincManifest(bundle_name, version, self)
        if self._manifests.get(bundle_name) is None:
            self._manifests[bundle_name] = {} # create version dict
        self._manifests[bundle_name][int(version)] = manifest
        return manifest

    def versions_for_bundle(self, bundle_name):
        manifests_by_version = self._manifests.get(bundle_name)
        if manifests_by_version is None:
            return []
        return sorted(manifests_by_version.keys())

    def get_bundle(self, name, version):
        versions = self._manifests.get(name)
        if versions is None:
            return None
        return versions.get(int(version))

    def bundle_names(self):
        return self._manifests.keys()

    def create_bundle_version(self, bundle_name, src_dir):
        op = CreateBundleVersionOperation(self, bundle_name, src_dir)
        op.run()

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

    commands = {
            "repo" : ("create", "verify"),
            "bundle": ("create", "commit"),
            }

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

    command = args[0]
    if command == "repo:verify":
        if len(args) < 2:
            parser.print_usage()
            exit(1)
        path = args[1]
        _cmd_verify(path)
        exit(0)
    elif command == "repo:create": # TODO: better command parsing
        if len(args) < 2:
            parser.print_usage()
            exit(1)
        path = args[1]
        init_repo_at_path(path)
        exit(0)

    if len(args) != 2:
        parser.print_usage()
        exit(1)

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

