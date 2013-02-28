import sys
import os
import logging
import argparse
import json
import errno
from os.path import join as pjoin

from .utils import sha1_for_path, canonical_path, makedirs, mygzip
from .models import (ZincIndex, load_index, ZincError, ZincErrors,
        ZincOperation, ZincConfig, load_config, ZincManifest, load_manifest,
        CreateBundleVersionOperation, ZincCatalog, create_catalog_at_path,
        ZincFlavorSpec)
from .defaults import defaults
from .pathfilter import PathFilter
from .tasks.bundle_clone import ZincBundleCloneTask
from .client import ZincClientConfig

logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s')

DEFAULT_CONFIG_PATH='~/.zinc'


### Helpers ##################################################################

def load_config(path):

    # if it's the default config path and it doesn't exist, just return and
    # empty config

    path = canonical_path(path)
    is_default = path == canonical_path(DEFAULT_CONFIG_PATH)
    exists = os.path.exists(path)

    if is_default and not exists:
        return ZincClientConfig()

    if not exists:
        sys.exit("File not found: %s" % (path))

    return ZincClientConfig.from_path(path)


### Commands #################################################################

def cmd_catalog_create(args, config):
    dest = args.catalog
    if dest is None:
        dest = './%s' % (args.catalog_id)
    create_catalog_at_path(dest, args.catalog_id)

def cmd_catalog_clean(args, config):
    catalog = ZincCatalog(args.catalog)
    catalog.clean(dry_run=not args.force)

def cmd_catalog_list(args, config):
    catalog = ZincCatalog(args.catalog)
    distro = args.distro
    print_versions = not args.no_versions
    for bundle_name in catalog.bundle_names():
        if distro and distro not in catalog.index.distributions_for_bundle(bundle_name):
            continue
        distros = catalog.index.distributions_for_bundle_by_version(bundle_name)
        versions = catalog.versions_for_bundle(bundle_name)
        version_strings = list()
        for version in versions:
            version_string = str(version)
            if distros.get(version) is not None:
                distro_string = "(%s)" % (", ".join(distros.get(version)))
                version_string += '=' + distro_string
            version_strings.append(version_string)

        final_version_string =  "[%s]" %(", ".join(version_strings))
        if print_versions:
            print "%s %s" % (bundle_name, final_version_string)
        else:
            print "%s" % (bundle_name)

def cmd_bundle_list(args, config):
    catalog = ZincCatalog(args.catalog)
    bundle_name = args.bundle_name
    version = int(args.version)
    manifest = catalog.manifest_for_bundle(bundle_name, version=version)
    all_files = sorted(manifest.get_all_files())
    for f in all_files:
        if args.sha:
            print f, 'sha=%s' % (manifest.sha_for_file(f))
        else:
            print f

def cmd_bundle_update(args, config):
    flavors = None
    if args.flavors is not None:
        with open(args.flavors) as f:
            flavors_dict = json.load(f)
            flavors = ZincFlavorSpec.from_dict(flavors_dict)

    catalog = ZincCatalog(args.catalog)
    bundle_name = args.bundle_name
    path = args.path
    force = args.force
    skip_master_archive = args.skip_master_archive
    manifest = catalog.create_bundle_version(
            bundle_name, path, flavor_spec=flavors, force=force,
            skip_master_archive=skip_master_archive)
    print "Updated %s v%d" % (manifest.bundle_name, manifest.version)

def cmd_bundle_clone(args, config):
    catalog = ZincCatalog(args.catalog)

    task = ZincBundleCloneTask()
    task.catalog = catalog
    task.bundle_name = args.bundle_name
    task.version = int(args.version)
    task.flavor = args.flavor
    task.output_path = args.path

    task.run()

def cmd_bundle_delete(args, confg):
    bundle_name = args.bundle_name
    version = args.version
    catalog = ZincCatalog(args.catalog)
    dry_run = args.dry_run
    if version == 'all':
        versions_to_delete = catalog.versions_for_bundle(bundle_name)
    elif version == 'unreferenced':
        all_versions = catalog.versions_for_bundle(bundle_name)
        referenced_versions = catalog.index.distributions_for_bundle_by_version(bundle_name).keys()
        versions_to_delete = [v for v in all_versions if v not in referenced_versions]
    else:
        versions_to_delete = [version]

    if len(versions_to_delete) == 0:
        print 'Nothing to do'
    elif len(versions_to_delete) > 1:
        verb = 'Would remove' if dry_run else 'Removing'
        print "%s versions %s" % (verb, versions_to_delete)

    if not dry_run:
        for v in versions_to_delete:
            catalog.delete_bundle_version(bundle_name, int(v))

def cmd_distro_update(args, config):
    catalog = ZincCatalog(args.catalog)
    bundle_name = args.bundle_name
    distro_name = args.distro_name
    bundle_version_arg = args.version
    if bundle_version_arg == "latest":
        bundle_version = catalog.versions_for_bundle(bundle_name)[-1]
    elif bundle_version_arg.startswith('@'):
        source_distro = bundle_version_arg[1:]
        bundle_version = catalog.index.version_for_bundle(bundle_name, source_distro)
    else:
        bundle_version = int(bundle_version_arg)
    catalog.update_distribution(
            distro_name, bundle_name, bundle_version)

def cmd_distro_delete(args, config):
    catalog = ZincCatalog(args.catalog)
    bundle_name = args.bundle_name
    distro_name = args.distro_name
    catalog.delete_distribution(distro_name, bundle_name)

## TODO: replace this
#def _cmd_verify(path):
#    catalog = ZincCatalog(path)
#    results = catalog.verify()
#    error_count = total_count = 0
#    for (file, error) in results.items():
#        if error.code != ZincErrors.OK.code:
#            print error.message + " : " + file
#            error_count = error_count + 1
#        total_count = total_count + 1
#    print "Verified %d files, %d errors" % (total_count, error_count)


### Main #####################################################################

def main():
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-C', '--config', default=DEFAULT_CONFIG_PATH,
            help='Config file path. Defaults to \'%s\'.' % (DEFAULT_CONFIG_PATH))

    subparsers = parser.add_subparsers(
            title='subcommands',
            description='valid subcommands',
            help='additional help')

    add_catalog_arg = lambda parser, required=True: parser.add_argument(
            '-c', '--catalog', required=required, help='Catalog reference.')
    add_bundle_arg = lambda parser, required=True: parser.add_argument(
            '-b', '--bundle', required=True, help='Bundle name.')
    add_distro_arg = lambda parser, required=True: parser.add_argument(
            '-d', '--distro', required=required, help='Name of the distro.')
    add_version_arg = lambda parser, required=True: parser.add_argument(
            '-v', '--version', required=required, help='Version.')

    # catalog:create
    parser_catalog_create = subparsers.add_parser(
            'catalog:create', help='catalog:create help')
    parser_catalog_create.add_argument('catalog_id')
    parser_catalog_create.add_argument('-c', '--catalog',
            help='Destination path. Defaults to "./<catalog_id>"')
    parser_catalog_create.set_defaults(func=cmd_catalog_create)

    # catalog:clean
    parser_catalog_clean = subparsers.add_parser(
            'catalog:clean', help='catalog:clean help')
    add_catalog_arg(parser_catalog_clean)
    parser_catalog_clean.add_argument(
            '-f', '--force', default=False, action='store_true', 
            help='This command does a dry run by default. Specifying this flag '
            'will cause files to actually be removed.')
    parser_catalog_clean.set_defaults(func=cmd_catalog_clean)

    # catalog:list
    parser_catalog_list = subparsers.add_parser(
            'catalog:list', help='List contents of catalog')
    add_catalog_arg(parser_catalog_list)
    add_distro_arg(parser_catalog_list, required=False)
    parser_catalog_list.add_argument(
            '--no-versions', default=False, action='store_true', 
            help='Omit version information for bundles.')
    parser_catalog_list.set_defaults(func=cmd_catalog_list)

    # bundle:list
    parser_bundle_list = subparsers.add_parser(
            'bundle:list', help='List contents of a bundle')
    add_catalog_arg(parser_bundle_list)
    add_bundle_arg(parser_bundle_list)
    add_version_arg(parser_bundle_list)
    parser_bundle_list.add_argument(
            '--sha', default=False, action='store_true', 
            help='Print file SHA hash.')
    parser_bundle_list.set_defaults(func=cmd_bundle_list)

    # bundle:update
    parser_bundle_update = subparsers.add_parser(
            'bundle:update', help='bundle:update help')
    add_catalog_arg(parser_bundle_update)
    add_bundle_arg(parser_bundle_update)
    parser_bundle_update.add_argument(
            '-p', '--path', required=True,
            help='Path to files for this bundle.')
    parser_bundle_update.add_argument(
            '--flavors', help='Flavor spec path. Should be JSON.')
    parser_bundle_update.add_argument(
            '--skip-master-archive', default=False, action='store_true', 
            help='Skips creating master archive if flavors are specified.')
    parser_bundle_update.add_argument(
            '-f', '--force', default=False, action='store_true', 
            help='Update bundle even if no files changed.')
    parser_bundle_update.set_defaults(func=cmd_bundle_update)

    # bundle:clone
    parser_bundle_clone = subparsers.add_parser(
            'bundle:clone', help='Clones a bundle to a local directory.')
    add_catalog_arg(parser_bundle_clone)
    add_bundle_arg(parser_bundle_clone)
    add_version_arg(parser_bundle_clone)
    parser_bundle_clone.add_argument(
            '-p', '--path', required=True,
            help='Destination path for bundle clone.')
    parser_bundle_clone.add_argument(
            '--flavor', help='Name of flavor.')
    parser_bundle_clone.set_defaults(func=cmd_bundle_clone)

    # bundle:delete
    parser_bundle_delete = subparsers.add_parser(
            'bundle:delete', help='bundle:delete help')
    add_catalog_arg(parser_bundle_delete)
    add_bundle_arg(parser_bundle_delete)
    add_version_arg(parser_bundle_delete)
    parser_bundle_delete.add_argument(
            '-n', '--dry-run', default=False, action='store_true', 
            help='Dry run. Don\' actually delete anything.')
    parser_bundle_delete.set_defaults(func=cmd_bundle_delete)

    # distro:update
    parser_distro_update = subparsers.add_parser(
            'distro:update', help='distro:update help')
    add_catalog_arg(parser_distro_update)
    add_bundle_arg(parser_distro_update)
    add_version_arg(parser_distro_update)
    add_distro_arg(parser_distro_update)
    parser_distro_update.set_defaults(func=cmd_distro_update)

    # distro:delete
    parser_distro_delete = subparsers.add_parser(
            'distro:delete', help='distro:delete help')
    add_catalog_arg(parser_distro_delete)
    add_bundle_arg(parser_distro_delete)
    add_distro_arg(parser_distro_delete)
    parser_distro_delete.set_defaults(func=cmd_distro_delete)

    args = parser.parse_args()
    config = load_config(args.config)
    args.func(args, config)


if __name__ == "__main__":
    main()

