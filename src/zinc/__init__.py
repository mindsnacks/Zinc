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

def sha1_for_path(path):
    sha1 = hashlib.sha1()
    f = open(path, 'rb')
    try:
        sha1.update(f.read())
    finally:
        f.close()
    return sha1.hexdigest()

class ZincError(object):
    def __init__(self, code, message):
        self.code = code
        self.message = message

# TODO: might be worst python ever
class ZincErrors(object):
    OK = ZincError(0, "OK")
    INCORRECT_SHA = ZincError(1, "SHA did not match")
    DOES_NOT_EXIST = ZincError(2, "Does not exist")

class ZincInstance(object):

    def _read_info_file(self):
        info_path = pjoin(self.path, "info.json")
        info_file = open(info_path, "r")
        self._info_dict = json.load(info_file)
        info_file.close()

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
        self._read_info_file()
        # TODO: check format, just assume 1 for now
        self._read_manifests()
        self._loaded = True

    def __init__(self, path):
        self._loaded = False
        self.path = os.path.realpath(path)
        #self.available_versions = []
        #self.format = ZINC_FORMAT
        self._load()

    def clean(self):
        pass

    def available_versions(self):
        return self.manifests.keys()

    def latest_version(self):
        return sorted(self.available_versions())[-1]

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


def copy_contents_only(path):
    return path[-1] == os.sep

def create_dir_if_needed(path):
    try:
        os.makedirs(path)
    except os.error as e: 
        if e.errno != errno.EEXIST:
            raise e

# ##commands
#
# $ zinc init
#
# $ zinc update zincspec.json
# 
# $ zinc publish zincspec.json

def _verify(path):
    path = os.path.realpath(path)
    zinc_instance = ZincInstance(path)
    results = zinc_instance.verify()
    error_count = total_count = 0
    for (file, error) in results.items():
        if error.code != ZincErrors.OK.code:
            print error.message + " : " + file
            error_count = error_count + 1
        total_count = total_count + 1
    print "Verified %d files, %d errors" % (total_count, error_count)


def main():
    commands = ("verify", "update", "clean")
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
    if command == 'verify':
        if len(args) < 2:
            parser.print_usage()
            exit(1)
        path = args[1]
        _verify(path)
        exit(0)


    if len(args) != 2:
        parser.print_usage()
        exit(1)

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

    # write info file
    info = {
            'zinc_fileormat' :  ZINC_FORMAT,
            }
    info_path = pjoin(dst, "info.json")
    info_file = open(info_path, "w")
    info_file.write(json.dumps(info, indent=2, sort_keys=True))
    info_file.close()


if __name__ == "__main__":
    main()
