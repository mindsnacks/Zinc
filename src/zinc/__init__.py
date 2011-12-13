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

    def write(self, path):
        index_file = open(path, 'w')
        index_file.write(json.dumps(self.__dict__, indent=2, sort_keys=True))
        index_file.close()

def load_index(path):
    index_file = open(path, 'r')
    dict = json.load(index_file)
    index_file.close()
    index = ZincIndex()
    index.format = dict['format']
    return index

       
### ZincConfig ###############################################################

class ZincConfig(object):

    def __init__(self):
        pass

    def write(self, path):
        config_file = open(path, 'w')
        config_file.write('')
        config_file.close()

### Bundle ###################################################################

class ZincBundle(object):

    class NewVersionOperation(ZincOperation):

        def __init__(self, bundle):
            self.bundle = bundle
            self._paths = []

        # TODO: make this accept a list
        def add_path(self, path):
            self._paths.append(canonical_path(path))

    def __init__(self, name, repo=None):
        self.name = name
        self._versions = ()
        self.current_version = None
        self.repo = repo

    def add_version(self):
        if self.repo == None:
            raise Exception("must have a repo")
        return NewVersionOperation(self)


### Repo #####################################################################

def create_repo_at_path(path):

    path = canonical_path(path)

    zinc_dir = pjoin(path, "zinc")
    os.makedirs(zinc_dir)
        
    zinc_config_path = pjoin(zinc_dir, "config")
    ZincConfig().write(zinc_config_path)

    zinc_index_path = pjoin(path, "index.json")
    ZincIndex().write(zinc_index_path)

    # TODO: check exceptions?

    return ZincRepo(path)

class ZincRepo(object):

    def _read_index_file(self):
        index_path = pjoin(self.path, "index.json")
        self.index = load_index(index_path)
        if self.index.format != ZINC_FORMAT:
            raise Exception("Incompatible format %s" % (self.index.format))

    def _read_manifests(self):
        self.manifests = dict()
        versions_path = pjoin(self.path, "versions")
        for root, dirs, files in os.walk(versions_path):
            for f in files:
                if f == "manifest.json":
                    manifest_path = pjoin(root, f)
                    manifest_file = open(manifest_path, "r")
                    manifest_dict = json.load(manifest_file)
                    manifest_file.close()
                    manifest_version_major = manifest_dict.get("version").split(".")[0]
                    self.manifests[manifest_version_major] = manifest_dict

    def _load(self):
        self._read_index_file()
        # TODO: check format, just assume 1 for now
        self._read_manifests()
        self._loaded = True

    def __init__(self, path):
        self._loaded = False
        self.path = canonical_path(path)
        self._bundles = {}
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

    def _path_for_object_with_sha(self, sha):
        return pjoin(self._objects_dir(), sha)

    def import_path(self, src_path):
        src_path_sha = sha1_for_path(src_path)
        copyfile(src_path, self._path_for_object_with_sha(src_path_sha))
        
    def lock(self):
        pass
    
    def unlock(self):
        pass

    def clean(self):
        pass

    def path_for_file(self, file, version=None):
        if version == None:
            version = self.latest_version()

        manifest = self.manifests.get(version)
        sha = manifest.get("files").get(file)
        (basename, ext) = os.path.splitext(file)
        zinc_path = pjoin("objects", "%s+%s%s" % (basename, sha, ext))
        return zinc_path

    def verify(self):
        if not self._loaded:
            raise Exception("not loaded")
            # TODO: better exception
            # TODO: wrap in decorator?

        results_by_file = dict()
        for version, manifest in self.manifests.items():
            files = manifest.get("files")
            for file, sha in files.items():
                full_path = pjoin(self.path, self.path_for_file(file, version))
                logging.debug("verifying %s" % full_path)
                if not os.path.exists(full_path):
                    results_by_file[file] = ZincErrors.DOES_NOT_EXIST
                elif sha1_for_path(full_path) != sha:
                    results_by_file[file] = ZincErrors.INCORRECT_SHA
                else:
                    # everything is ok alarm
                    results_by_file[file] = ZincErrors.OK
        return results_by_file

    def get_bundle(self, bundle_name):
        return self._bundles.get(bundle_name)

    def add_bundle(self, bundle_name):
        bundle = self.get_bundle(bundle_name)
        if bundle is not None:
            raise ValueError("Bundle already exists")
            return None
        bundle = ZincBundle(bundle_name, self)
        self._bundles[bundle_name] = bundle
        return bundle

    def bundle_names(self):
        return self._bundles.keys()



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

