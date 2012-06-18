import logging
import optparse
import os
import json
from os.path import join as pjoin
import tarfile

import errno
from utils import sha1_for_path, canonical_path, makedirs, mygzip

from .models import (ZincIndex, load_index, ZincError, ZincErrors,
        ZincOperation, ZincConfig, load_config, ZincManifest, load_manifest,
        CreateBundleVersionOperation, ZincCatalog, create_catalog_at_path)
from .defaults import defaults


logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s')


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
            print "bundle:update <bundle id> <path>"
            exit(1)
        catalog = ZincCatalog(".")
        bundle_id = args[1]
        path = args[2]
        manifest = catalog.create_bundle_version(bundle_id, path)
        print "Updated %s v%d" % (manifest.bundle_id, manifest.version)
        exit(0)
    elif command == "distro:update": 
        if len(args) < 4:
            #parser.print_usage()
            print "distro:update <distro name> <bundle id> <bundle version>"
            exit(1)
        catalog = ZincCatalog(".")
        distro_name = args[1]
        bundle_id = args[2]
        bundle_version = args[3]
        catalog.update_distribution(distro_name, bundle_id, bundle_version)
        exit(0)


if __name__ == "__main__":
    main()

