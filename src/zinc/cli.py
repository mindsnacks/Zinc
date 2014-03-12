# -*- coding: utf-8 -*-

import argparse
import json
import logging
import os
import sys
from urlparse import urlparse
from functools import wraps

from zinc.utils import canonical_path
from zinc.models import ZincFlavorSpec
from zinc.formats import Formats
from zinc.client import ZincClientConfig
import zinc.client as client
import zinc.helpers as helpers
import zinc.utils as utils

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = '~/.zinc'


### Helpers ##################################################################

def load_config(path):

    path = canonical_path(path)
    is_default = path == canonical_path(DEFAULT_CONFIG_PATH)
    exists = os.path.exists(path)

    # if it's the default config path and it doesn't exist, just return an
    # empty config
    if is_default and not exists:
        return ZincClientConfig()

    if not exists:
        sys.exit("File not found: %s" % (path))

    return ZincClientConfig.from_path(path)


def set_loglevel(cargs):
    # TODO: this could be a lot cleaner

    if cargs.loglevel == 'debug':
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
    else:
        logging.basicConfig(level=logging.INFO,
                            format='[%(levelname)s] %(message)s')


def catalog_from_config(config, catalog_ref):

    if config.bookmarks.get(catalog_ref):
        catalog_id = config.bookmarks[catalog_ref]['catalog_id']
        coordinator_info = config.coordinators[config.bookmarks[catalog_ref]['coordinator']]
        storage_info = config.storages[config.bookmarks[catalog_ref]['storage']]
        return catalog_id, coordinator_info, storage_info

    return None, None, None  # seems a bit silly


def resolve_storage_info(config, storage_ref):

    # first check if it's already a valid URL
    urlcomps = urlparse(storage_ref)
    if not urlcomps.scheme == '':
        return {'url': storage_ref}

    # check if it's in the config
    storage_info = config.storages.get(storage_ref)
    if storage_info is not None:
        return storage_info

    # assume it's a path and convert to file url
    path = canonical_path(storage_ref)
    return {'url': 'file://%s' % (path)}


def get_catalog(config, cargs):
    catalog_id, coordinator_info, storage_info = catalog_from_config(config, cargs.catalog)
    timeout = vars(cargs).get('timeout')
    if coordinator_info is not None and storage_info is not None:
        service = client.connect(coordinator_info=coordinator_info, storage_info=storage_info)
        catalog = service.get_catalog(id=catalog_id, lock_timeout=timeout)
    else:
        # TODO: not sure if this is correct for general case
        service = client.connect(cargs.catalog)
        catalog = service.get_catalog(lock_timeout=timeout)
    return catalog


def parse_single_version_ish(catalog, bundle_name, version_string):

    if version_string in client.SymbolicSingleBundleVersions or \
            version_string.startswith(client.BundleVersionDistroPrefix):
        bundle_version = version_string

    else:
        bundle_version = int(version_string)

    return bundle_version


def parse_multi_version_ish(catalog, bundle_name, version_list):

    parsed_version_list = list()

    for version in version_list:
        if version in client.SymbolicBundleVersions:
            parsed_version = version
        else:
            parsed_version = int(version)
        parsed_version_list.append(parsed_version)

    return parsed_version_list


### Client Commands #################################################################
# TODO: move to zinc.client ?


def bundle_update(catalog, bundle_name, path, flavors=None, force=False,
                  skip_master_archive=True):
    manifest = client.create_bundle_version(catalog, bundle_name, path,
                                            flavor_spec=flavors, force=force,
                                            skip_master_archive=skip_master_archive)
    #print "Updated %s v%d" % (manifest.bundle_name, manifest.version)
    # TODO: add some nice human readable and machine readable output options
    print "%d" % (manifest.version)


def bundle_delete(catalog, bundle_name, versions, dry_run=False):

    if len(versions) == 0:
        print 'Nothing to do'
    else:
        verb = 'Would remove' if dry_run else 'Removing'
        print "%s versions %s" % (verb, versions)

    if not dry_run:
        client.delete_bundle_versions(catalog, bundle_name, versions)


def distro_update(catalog, bundle_name, distro_name, version,
        save_previous=True):

    errors = helpers.distro_name_errors(distro_name)
    if len(errors) > 0:
        for e in errors:
            log.error(e)
        sys.exit()

    client.update_distribution(catalog, distro_name, bundle_name, version,
            save_previous=save_previous)


def distro_delete(catalog, distro_name, bundle_name):
    catalog.delete_distribution(distro_name, bundle_name)


def flavorspec_list(catalog):
    for n in catalog.get_flavorspec_names():
        print n


def flavorspec_update(catalog, path, name):
    catalog.update_flavorspec_from_path(path, name=name)


def flavorspec_delete(catalog, name):
    catalog.delete_flavorspec(name)


### Subcommand Parsing #################################################################

def cli_cmd(f):
    @wraps(f)
    def func(self, *args, **kwargs):
        results = f(self, *args, **kwargs)
        for result in results:
            print result.format(args[0].format)

        errors = results.errors()
        if len(errors) > 0:
            print "\n\nERRORS:"
            for error in errors:
                print error.format(args[0].format)

    return func


@cli_cmd
def subcmd_catalog_list(config, cargs):
    catalog = get_catalog(config, cargs)
    return client.catalog_list(catalog, distro=cargs.distro,
                               print_versions=not cargs.no_versions)


def subcmd_catalog_create(config, cargs):
    storage_ref = cargs.storage
    catalog_id = cargs.catalog_id
    storage_info = resolve_storage_info(config, storage_ref)
    client.create_catalog(catalog_id=catalog_id, storage_info=storage_info)
    print "Catalog '%s' successfully created." % (catalog_id)


def subcmd_catalog_clean(config, cargs):
    catalog = get_catalog(config, cargs)
    catalog.clean(dry_run=not cargs.force)


@cli_cmd
def subcmd_bundle_list(config, cargs):
    catalog = get_catalog(config, cargs)
    bundle_name = cargs.bundle
    version = parse_single_version_ish(catalog, bundle_name, cargs.version)
    print_sha = cargs.sha
    flavor_name = cargs.flavor
    return client.bundle_list(catalog, bundle_name, version,
            print_sha=print_sha, flavor_name=flavor_name)


def subcmd_bundle_update(config, cargs):
    catalog = get_catalog(config, cargs)

    flavors = None
    if cargs.flavors is not None:
        try:
            with open(cargs.flavors) as f:
                flavors_dict = json.load(f)
                flavors = ZincFlavorSpec.from_dict(flavors_dict)
        except IOError:
            flavors = catalog.get_flavorspec(cargs.flavors)

        assert flavors is not None

    bundle_name = cargs.bundle
    path = cargs.path
    force = cargs.force
    skip_master_archive = cargs.skip_master_archive

    bundle_update(catalog, bundle_name, path, flavors=flavors, force=force,
                  skip_master_archive=skip_master_archive)


def subcmd_bundle_clone(config, cargs):
    catalog = get_catalog(config, cargs)
    bundle_name = cargs.bundle
    bundle_id = helpers.make_bundle_id(catalog.id, bundle_name)
    version = parse_single_version_ish(catalog, bundle_name, cargs.version)
    flavor = cargs.flavor
    root_path = cargs.path

    if cargs.no_versions:
        bundle_dir_name = bundle_id
    else:
        bundle_dir_name = None

    client.clone_bundle(catalog, bundle_name, version, root_path=root_path,
                        bundle_dir_name=bundle_dir_name, flavor=flavor)

    if cargs.include_manifest:
        if cargs.no_versions:
            manifest_name = '%s.json' % (bundle_id)
        else:
            manifest_name = '%s.json' % (helpers.make_bundle_descriptor(bundle_id, version))

        manifest_dest_path = os.path.join(root_path, manifest_name)
        manifest_src_path = catalog.path_helper.path_for_manifest_for_bundle_version(bundle_name, version)
        _dump_json(catalog, manifest_src_path, dest_path=manifest_dest_path)


def subcmd_bundle_delete(config, cargs):
    catalog = get_catalog(config, cargs)
    bundle_name = cargs.bundle
    versions = parse_multi_version_ish(catalog, bundle_name, cargs.versions)
    dry_run = cargs.dry_run
    bundle_delete(catalog, bundle_name, versions, dry_run=dry_run)


@cli_cmd
def subcmd_bundle_verify(config, cargs):
    catalog = get_catalog(config, cargs)
    bundle_name = cargs.bundle
    version_ish = parse_single_version_ish(catalog, bundle_name, cargs.version)
    return client.bundle_verify(catalog, bundle_name, version_ish)


def subcmd_distro_update(config, cargs):

    catalog = get_catalog(config, cargs)
    bundle_name = cargs.bundle
    distro_name = cargs.distro
    version = parse_single_version_ish(catalog, bundle_name, cargs.version)
    save_previous = not cargs.no_prev_distro
    distro_update(catalog, bundle_name, distro_name, version,
                  save_previous=save_previous)


def subcmd_distro_delete(config, cargs):
    catalog = get_catalog(config, cargs)
    bundle_name = cargs.bundle
    distro_name = cargs.distro
    distro_delete(catalog, distro_name, bundle_name)


def subcmd_flavorspec_list(config, cargs):
    catalog = get_catalog(config, cargs)
    flavorspec_list(catalog)


def subcmd_flavorspec_update(config, cargs):
    catalog = get_catalog(config, cargs)
    name = cargs.name
    path = cargs.path
    flavorspec_update(catalog, path, name)


def subcmd_flavorspec_delete(config, cargs):
    catalog = get_catalog(config, cargs)
    flavorspec_delete(catalog, cargs.name)


@cli_cmd
def subcmd_catalog_verify(config, cargs):
    catalog = get_catalog(config, cargs)
    should_lock = cargs.lock
    return client.verify_catalog(catalog, should_lock=should_lock)


def _dump_json(catalog, subpath, dest_path=None, should_decompress=True, gzip=True):

    if gzip:
        subpath = helpers.append_file_extension_for_format(subpath, Formats.GZ)

    data = catalog._read(subpath)  # TODO: fix private method references

    if gzip and should_decompress:
        out = utils.gunzip_bytes(data)
    else:
        out = data

    if dest_path is not None:
        with open(dest_path, 'w+b') as f:
            f.write(out)
        log.info('Wrote %s' % (dest_path))
    else:
        print out


def _dump_get_dest_path(subpath, cargs):
    if cargs.remote_name:
        return os.path.split(subpath)[-1]
    else:
        return None


def subcmd_dump_index(config, cargs):

    # TODO: this should not load the catalog - it should just pull the file from
    # the storage

    catalog = get_catalog(config, cargs)
    subpath = catalog.path_helper.path_for_index()
    dest_path = _dump_get_dest_path(subpath, cargs)

    _dump_json(catalog, subpath, should_decompress=not cargs.no_decompress,
               dest_path=dest_path)


def subcmd_dump_manifest(config, cargs):

    # TODO: this should not load the catalog - it should just pull the file from
    # the storage

    catalog = get_catalog(config, cargs)
    bundle_name = cargs.bundle
    version = parse_single_version_ish(catalog, bundle_name, cargs.version)

    subpath = catalog.path_helper.path_for_manifest_for_bundle_version(bundle_name, version)
    dest_path = _dump_get_dest_path(subpath, cargs)

    _dump_json(catalog, subpath, should_decompress=not cargs.no_decompress,
               dest_path=dest_path)


def subcmd_dump_flavorspec(config, cargs):
    catalog = get_catalog(config, cargs)
    subpath = catalog.path_helper.path_for_flavorspec_name(cargs.name)
    dest_path = _dump_get_dest_path(subpath, cargs)
    _dump_json(catalog, subpath, dest_path=dest_path, gzip=False)


def subcmd_debug_flavors(config, cargs):
    flavors = ZincFlavorSpec.from_path(cargs.flavors)
    for flavor_name in flavors.flavors:
        print '[%s]' % flavor_name
        filter = flavors.filter_for_flavor(flavor_name)
        src_dir = cargs.path
        for root, dirs, files in os.walk(src_dir):
            for f in files:
                #if f in IGNORE: continue # TODO: integrate ignore system
                rel_dir = root[len(src_dir) + 1:]
                rel_path = os.path.join(rel_dir, f)
                matched = filter.match(rel_path)
                print '%s %s' % ('+' if matched else '-', rel_path)
        print ""  # blank line


### Main #####################################################################

def main():
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-C', '--config',
                        default=DEFAULT_CONFIG_PATH,
                        help='Config file path. Defaults to \'%s\'.' % (DEFAULT_CONFIG_PATH))

    parser.add_argument('--format',
                        choices=(client.OutputType.PRETTY,
                                 client.OutputType.JSON),
                        default=client.OutputType.PRETTY)

    # TODO: embetter this
    parser.add_argument('--loglevel', default='error',
                        choices=('info', 'debug'),
                        help='Log level.')

    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')

    add_catalog_arg = lambda parser, required=True: parser.add_argument(
        '-c', '--catalog', required=required, help='Catalog reference.')
    add_bundle_arg = lambda parser, required=True: parser.add_argument(
        '-b', '--bundle', required=True, help='Bundle name.')
    add_distro_arg = lambda parser, required=True: parser.add_argument(
        '-d', '--distro', required=required, help='Name of the distro.')
    add_single_version_arg = lambda parser, required=True: parser.add_argument(
        '-v', '--version', required=required, help='Version.')
    add_multiple_versions_arg = lambda parser, required=True: \
        parser.add_argument('-v', '--versions', nargs='+', required=required,
            help='Versions, separated by spaces.')
    add_timeout_arg = lambda parser, required=False: parser.add_argument(
        '--timeout', required=required, default=None,
        help='Timeout for acquiring catalog locks. 0 = wait forever.')
    add_remote_name_arg = lambda parser, required=False: parser.add_argument(
        '-O', '--remote-name', required=required, default=False,
        action='store_true', help='Write to file using remote name instead of stdout.')
    add_no_decompress_arg = lambda parser, required=False: parser.add_argument(
        '--no-decompress', required=required, default=False,
        action='store_true', help='Don\'t decompress file after downloading.')

    # catalog:create
    parser_catalog_create = subparsers.add_parser('catalog:create',
                                                  help='catalog:create help')
    parser_catalog_create.add_argument('catalog_id')
    parser_catalog_create.add_argument('-s', '--storage',
                                       help='Storage descriptor. Defaults to "file://."',
                                       default='.')
    parser_catalog_create.set_defaults(func=subcmd_catalog_create)

    # catalog:clean
    parser_catalog_clean = subparsers.add_parser('catalog:clean',
                                                 help='catalog:clean help')
    add_catalog_arg(parser_catalog_clean)
    add_timeout_arg(parser_catalog_clean)
    parser_catalog_clean.add_argument('-f', '--force',
                                      default=False,
                                      action='store_true',
                                      help='This command does a dry run by default. Specifying this flag will cause files to actually be removed.')
    parser_catalog_clean.set_defaults(func=subcmd_catalog_clean)

    # catalog:verify
    parser_catalog_verify = subparsers.add_parser('catalog:verify',
                                                  help='catalog:verify help')
    add_catalog_arg(parser_catalog_verify)
    parser_catalog_verify.add_argument('--lock',
                                       default=False,
                                       action='store_true',
                                       help='Lock the catalog while verifying.')
    parser_catalog_verify.set_defaults(func=subcmd_catalog_verify)

    # catalog:list
    parser_catalog_list = subparsers.add_parser('catalog:list',
                                                help='List contents of catalog')
    add_catalog_arg(parser_catalog_list)
    add_distro_arg(parser_catalog_list, required=False)
    parser_catalog_list.add_argument('--no-versions',
                                     default=False,
                                     action='store_true',
                                     help='Omit version information for bundles.')
    parser_catalog_list.set_defaults(func=subcmd_catalog_list)

    # bundle:list
    parser_bundle_list = subparsers.add_parser('bundle:list',
                                               help='List contents of a bundle')
    add_catalog_arg(parser_bundle_list)
    add_bundle_arg(parser_bundle_list)
    add_single_version_arg(parser_bundle_list)
    parser_bundle_list.add_argument('--sha',
                                    default=False,
                                    action='store_true',
                                    help='Print file SHA hash.')
    parser_bundle_list.add_argument('--flavor',
                                    help='Name of flavor.')
    parser_bundle_list.set_defaults(func=subcmd_bundle_list)

    # bundle:update
    parser_bundle_update = subparsers.add_parser('bundle:update',
                                                 help='bundle:update help')
    add_catalog_arg(parser_bundle_update)
    add_timeout_arg(parser_bundle_update)
    add_bundle_arg(parser_bundle_update)
    parser_bundle_update.add_argument('-p', '--path',
                                      required=True,
                                      help='Path to files for this bundle.')
    parser_bundle_update.add_argument('--flavors',
                                      help='The flavorspec. Will first try to read as a path. If not found, will try to read from catalog by name.')
    parser_bundle_update.add_argument('-f', '--force',
                                      default=False,
                                      action='store_true',
                                      help='Update bundle even if no files changed.')

    parser_bundle_update_master_archive_group = parser_bundle_update.add_mutually_exclusive_group()
    parser_bundle_update_master_archive_group.add_argument('--skip-master-archive',
                                                           default=True,
                                                           action='store_true',
                                                           dest='skip_master_archive',
                                                           help='Skips creating master archive if flavors are specified. This is the default behavior.')
    parser_bundle_update_master_archive_group.add_argument('--include-master-archive',
                                                           default=False,
                                                           action='store_true',
                                                           dest='skip_master_archive',
                                                           help='Also creates master archive if flavors are specified.')

    parser_bundle_update.set_defaults(func=subcmd_bundle_update)

    # bundle:clone
    parser_bundle_clone = subparsers.add_parser('bundle:clone',
                                                help='Clones a bundle to a local directory.')
    add_catalog_arg(parser_bundle_clone)
    add_bundle_arg(parser_bundle_clone)
    add_single_version_arg(parser_bundle_clone)
    parser_bundle_clone.add_argument('-p', '--path',
                                     required=False,
                                     default='.',
                                     help='Destination path for bundle clone.')
    parser_bundle_clone.add_argument('--flavor',
                                     help='Name of flavor.')
    parser_bundle_clone.add_argument('--include-manifest',
                                     required=False,
                                     action='store_true',
                                     help='Also dump bundle manifest.')
    parser_bundle_clone.add_argument('--no-versions',
                                     default=False,
                                     action='store_true',
                                     help='Omit version when writing bundle file names.')
    parser_bundle_clone.set_defaults(func=subcmd_bundle_clone)

    # bundle:delete
    parser_bundle_delete = subparsers.add_parser('bundle:delete',
                                                 help='bundle:delete help')
    add_catalog_arg(parser_bundle_delete)
    add_timeout_arg(parser_bundle_delete)
    add_bundle_arg(parser_bundle_delete)
    add_multiple_versions_arg(parser_bundle_delete)
    parser_bundle_delete.add_argument('-n', '--dry-run',
                                      default=False,
                                      action='store_true',
                                      help='Dry run. Don\' actually delete anything.')
    parser_bundle_delete.set_defaults(func=subcmd_bundle_delete)

    # bundle:verify
    parser_bundle_verify = subparsers.add_parser('bundle:verify',
                                                 help='Verify all contents of a bundle.')
    add_catalog_arg(parser_bundle_verify)
    add_bundle_arg(parser_bundle_verify)
    add_single_version_arg(parser_bundle_verify)
    # TODO: allow for version OR distro
    parser_bundle_verify.set_defaults(func=subcmd_bundle_verify)

    # distro:update
    parser_distro_update = subparsers.add_parser('distro:update',
                                                 help='distro:update help')
    add_catalog_arg(parser_distro_update)
    add_timeout_arg(parser_distro_update)
    add_bundle_arg(parser_distro_update)
    add_single_version_arg(parser_distro_update)
    add_distro_arg(parser_distro_update)
    parser_distro_update.add_argument('--no-prev-distro',
                                      default=False,
                                      action='store_true',
                                      help='Do not preserve previous version for distro.')
    parser_distro_update.set_defaults(func=subcmd_distro_update)

    # distro:delete
    parser_distro_delete = subparsers.add_parser('distro:delete',
                                                 help='distro:delete help')
    add_catalog_arg(parser_distro_delete)
    add_timeout_arg(parser_distro_delete)
    add_bundle_arg(parser_distro_delete)
    add_distro_arg(parser_distro_delete)
    parser_distro_delete.set_defaults(func=subcmd_distro_delete)

    # flavorspec:list
    parser_flavorspec_list = subparsers.add_parser('flavorspec:list',
                                                   help='list stored flavorspecs')
    add_catalog_arg(parser_flavorspec_list)
    parser_flavorspec_list.set_defaults(func=subcmd_flavorspec_list)

    # flavorspec:update
    parser_flavorspec_update = subparsers.add_parser('flavorspec:update',
                                                     help='Update a stored flavorspec.')
    add_catalog_arg(parser_flavorspec_update)
    parser_flavorspec_update.add_argument('-p', '--path',
                                          required=True,
                                          help='Path to flavorspec file.')
    parser_flavorspec_update.add_argument('--name',
                                          required=False,
                                          help='flavorspec name. If ommitted, will use filename.')
    parser_flavorspec_update.set_defaults(func=subcmd_flavorspec_update)

    # flavorspec:delte
    parser_flavorspec_delete = subparsers.add_parser('flavorspec:delete',
                                                     help='Delete a stored flavorspec.')
    add_catalog_arg(parser_flavorspec_delete)
    parser_flavorspec_delete.add_argument('--name',
                                          required=True,
                                          help='flavorspec name.')
    parser_flavorspec_delete.set_defaults(func=subcmd_flavorspec_delete)

    # dump:index
    parser_dump_index = subparsers.add_parser('dump:index',
                                              help='Dump catalog index file (catalog.json).')
    add_catalog_arg(parser_dump_index)
    add_remote_name_arg(parser_dump_index)
    add_no_decompress_arg(parser_dump_index)
    parser_dump_index.set_defaults(func=subcmd_dump_index)

    # dump:manifest
    parser_dump_manifest = subparsers.add_parser('dump:manifest',
                                                 help='Dump a manifest file from a catalog.')
    add_catalog_arg(parser_dump_manifest)
    add_bundle_arg(parser_dump_manifest)
    add_single_version_arg(parser_dump_manifest)
    add_remote_name_arg(parser_dump_manifest)
    add_no_decompress_arg(parser_dump_manifest)
    parser_dump_manifest.set_defaults(func=subcmd_dump_manifest)

    # dump:flavorspec
    parser_dump_flavorspec = subparsers.add_parser('dump:flavorspec',
                                                   help='Dump a flavorspec file from a catalog.')
    parser_dump_flavorspec.add_argument('--name',
                                        required=True,
                                        help='flavorspec name.')
    add_catalog_arg(parser_dump_flavorspec)
    add_remote_name_arg(parser_dump_flavorspec)
    parser_dump_flavorspec.set_defaults(func=subcmd_dump_flavorspec)

    # debug:flavors
    parser_debug_flavors = subparsers.add_parser('debug:flavors',
                                                 help='Debug flavors.')
    parser_debug_flavors.add_argument('--flavors',
                                      required=True,
                                      help='Flavor spec path. Should be JSON.')
    parser_debug_flavors.add_argument('-p', '--path',
                                      required=False,
                                      default='.',
                                      help='Root folder to test')
    parser_debug_flavors.set_defaults(func=subcmd_debug_flavors)

    args = parser.parse_args()
    config = load_config(args.config)
    set_loglevel(args)
    args.func(config, args)


if __name__ == "__main__":
    main()
