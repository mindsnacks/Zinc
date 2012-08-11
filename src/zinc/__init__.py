import logging
import argparse
import os
import json
from os.path import join as pjoin
import tarfile

import errno
from utils import sha1_for_path, canonical_path, makedirs, mygzip

from .models import (ZincIndex, load_index, ZincError, ZincErrors,
        ZincOperation, ZincConfig, load_config, ZincManifest, load_manifest,
        CreateBundleVersionOperation, ZincCatalog, create_catalog_at_path,
        ZincFlavorSpec)
from .defaults import defaults
from .pathfilter import PathFilter


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

def catalog_create(args):
    dest = args.dest
    if dest is None:
        dest = './%s' % (args.catalog_id)
    create_catalog_at_path(dest, args.catalog_id)

def catalog_clean(args):
    catalog = ZincCatalog(args.catalog_path)
    catalog.clean(dry_run=not args.force)

def bundle_list(args):
    catalog = ZincCatalog(args.catalog_path)
    for bundle_name in catalog.bundle_names():
        versions = catalog.versions_for_bundle(bundle_name)
        print bundle_name, versions

def bundle_update(args):
    flavor_spec = None
    if args.flavor_spec is not None:
        with open(args.flavor_spec) as f:
            flavor_spec_dict = json.load(f)
            flavor_spec = ZincFlavorSpec.from_dict(flavor_spec_dict)

    catalog = ZincCatalog(args.catalog_path)
    bundle_name = args.bundle_name
    path = args.path
    force = args.force
    manifest = catalog.create_bundle_version(
            bundle_name, path, flavor_spec=flavor_spec, force=force)
    print "Updated %s v%d" % (manifest.bundle_name, manifest.version)

def bundle_delete(args):
    bundle_name = args.bundle_name
    version = args.version
    catalog = ZincCatalog(args.catalog_path)
    if version == 'all':
        versions_to_delete = catalog.versions_for_bundle(bundle_name)
    else:
        versions_to_delete = [version]
        for v in versions_to_delete:
            catalog.delete_bundle_version(bundle_name, int(v))

def distro_update(args):
    catalog = ZincCatalog(args.catalog_path)
    bundle_name = args.bundle_name
    distro_name = args.distro_name
    bundle_version = args.version
    if bundle_version != "latest":
        bundle_version = int(bundle_version)
    catalog.update_distribution(
            distro_name, bundle_name, bundle_version)

def distro_delete(args):
    catalog = ZincCatalog(args.catalog_path)
    bundle_name = args.bundle_name
    distro_name = args.distro_name
    catalog.delete_distribution(distro_name, bundle_name)


### Main #####################################################################

def main():
    parser = argparse.ArgumentParser(description='')

    subparsers = parser.add_subparsers(title='subcommands',
            description='valid subcommands',
            help='additional help')

    # catalog:create
    parser_catalog_create = subparsers.add_parser('catalog:create', help='catalog:create help')
    parser_catalog_create.add_argument('catalog_id')
    parser_catalog_create.add_argument('-c', '--catalog_path',
            help='Destination path. Defaults to "./<catalog_id"')
    parser_catalog_create.set_defaults(func=catalog_create)

    # catalog:clean
    parser_catalog_clean = subparsers.add_parser('catalog:clean',
            help='catalog:clean help')
    parser_catalog_clean.add_argument('-c', '--catalog_path', default='.',
            help='Destination path. Defaults to "."')
    parser_catalog_clean.add_argument('-f', '--force', default=False, action='store_true', 
            help='This command does a dry run by default. Specifying this flag '
            'will cause files to actually be removed.')
    parser_catalog_clean.set_defaults(func=catalog_clean)

    # bundle:list
    parser_bundle_list = subparsers.add_parser('bundle:list', help='bundle:list help')
    parser_bundle_list.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_bundle_list.set_defaults(func=bundle_list)

    # bundle:update
    parser_bundle_update = subparsers.add_parser('bundle:update', help='bundle:update help')
    parser_bundle_update.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_bundle_update.add_argument('--flavor_spec', 
            help='Flavor spec path. Should be JSON.')
    parser_bundle_update.add_argument('-f', '--force', default=False, action='store_true', 
            help='Update bundle even if no files changed.')
    parser_bundle_update.add_argument('bundle_name',
            help='Name of the bundle. Must not contain a period (.).')
    parser_bundle_update.add_argument('path',
            help='Path to files for this bundle.')
    parser_bundle_update.set_defaults(func=bundle_update)

    # bundle:delete
    parser_bundle_delete = subparsers.add_parser('bundle:delete', help='bundle:delete help')
    parser_bundle_delete.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_bundle_delete.add_argument('bundle_name',
            help='Name of the bundle. Must exist in catalog.')
    parser_bundle_delete.add_argument('version',
            help='Version number to delete or "all".')
    parser_bundle_delete.set_defaults(func=bundle_delete)

    # distro:update
    parser_distro_update = subparsers.add_parser('distro:update', help='distro:update help')
    parser_distro_update.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_distro_update.add_argument('bundle_name',
            help='Name of the bundle. Must exist in the catalog.')
    parser_distro_update.add_argument('distro_name',
            help='Name of the distro.')
    parser_distro_update.add_argument('version',
            help='Version number or "latest".')
    parser_distro_update.set_defaults(func=distro_update)

    # distro:delete
    parser_distro_delete = subparsers.add_parser('distro:delete', help='distro:delete help')
    parser_distro_delete.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_distro_delete.add_argument('bundle_name',
            help='Name of the bundle. Must exist in the catalog.')
    parser_distro_delete.add_argument('distro_name',
            help='Name of the distro.')
    parser_distro_delete.set_defaults(func=distro_delete)

    args = parser.parse_args()
    args.func(args)

#    if command == "catalog:verify":
#        if len(args) < 2:
#            parser.print_usage()
#            exit(2)
#        path = args[1]
#        _cmd_verify(path)
#        exit(0)

if __name__ == "__main__":
    main()

