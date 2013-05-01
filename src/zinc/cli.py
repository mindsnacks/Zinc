import argparse
import json
import logging
import os
import sys
from urlparse import urlparse

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


def set_loglevel(args):
    # TODO: this could be a lot cleaner

    if args.loglevel == 'debug':
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


def get_catalog(config, args):
    catalog_id, coordinator_info, storage_info = catalog_from_config(config, args.catalog)
    timeout = vars(args).get('timeout')
    if coordinator_info is not None and storage_info is not None:
        service = client.connect(coordinator_info=coordinator_info, storage_info=storage_info)
        catalog = service.get_catalog(id=catalog_id, lock_timeout=timeout)
    else:
        # TODO: not sure if this is correct for general case
        service = client.connect(args.catalog)
        catalog = service.get_catalog(lock_timeout=timeout)
    return catalog


def parse_single_version(catalog, bundle_name, version_string):

    if version_string == 'latest':
        index = catalog.get_index()
        bundle_version = index.versions_for_bundle(bundle_name)[-1]

    elif version_string.startswith('@'):
        source_distro = version_string[1:]
        bundle_version = catalog.index.version_for_bundle(bundle_name, source_distro)

    else:
        bundle_version = int(version_string)

    return bundle_version


def parse_multi_versions(catalog, bundle_name, version_string):

    if version_string == 'all':
        index = catalog.get_index()
        bundle_versions = index.versions_for_bundle(bundle_name)

    elif version_string == 'unreferenced':
        index = catalog.get_index()
        all_versions = index.versions_for_bundle(bundle_name)
        referenced_versions = catalog.index.distributions_for_bundle_by_version(bundle_name).keys()
        bundle_versions = [v for v in all_versions if v not in referenced_versions]

    else:
        bundle_versions = [parse_single_version(catalog, bundle_name, version_string)]

    return bundle_versions


### Client Commands #################################################################
# TODO: move to zinc.client ?

def catalog_list(catalog, distro=None, print_versions=True):
    index = catalog.get_index()
    bundle_names = sorted(index.bundle_names())
    for bundle_name in bundle_names:
        if distro and distro not in index.distributions_for_bundle(bundle_name):
            continue
        distros = index.distributions_for_bundle_by_version(bundle_name)
        versions = index.versions_for_bundle(bundle_name)
        version_strings = list()
        for version in versions:
            version_string = str(version)
            if distros.get(version) is not None:
                distro_string = "(%s)" % (", ".join(sorted(distros.get(version))))
                version_string += '=' + distro_string
            version_strings.append(version_string)

        final_version_string = "[%s]" % (", ".join(sorted(version_strings)))
        if print_versions:
            print "%s %s" % (bundle_name, final_version_string)
        else:
            print "%s" % (bundle_name)


def bundle_list(catalog, bundle_name, version, print_sha=False):
    manifest = catalog.manifest_for_bundle(bundle_name, version=version)
    all_files = sorted(manifest.get_all_files())
    for f in all_files:
        if print_sha:
            print f, 'sha=%s' % (manifest.sha_for_file(f))
        else:
            print f


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
        for v in versions:
            catalog.delete_bundle_version(bundle_name, v)


def distro_update(catalog, bundle_name, distro_name, version, save_previous=True):

    errors = helpers.distro_name_errors(distro_name)
    if len(errors) > 0:
        for e in errors:
            log.error(e)
        sys.exit()

    catalog.update_distribution(distro_name, bundle_name, version, save_previous=save_previous)


def distro_delete(catalog, distro_name, bundle_name):
    catalog.delete_distribution(distro_name, bundle_name)


### Subcommand Parsing #################################################################


def subcmd_catalog_list(config, args):
    catalog = get_catalog(config, args)
    distro = args.distro
    catalog_list(catalog, distro=distro, print_versions=not args.no_versions)


def subcmd_catalog_create(config, args):
    storage_ref = args.storage
    catalog_id = args.catalog_id
    storage_info = resolve_storage_info(config, storage_ref)
    client.create_catalog(catalog_id=catalog_id, storage_info=storage_info)
    print "Catalog '%s' successfully created." % (catalog_id)


def subcmd_catalog_clean(config, args):
    catalog = get_catalog(config, args)
    catalog.clean(dry_run=not args.force)


def subcmd_bundle_list(config, args):
    catalog = get_catalog(config, args)
    bundle_name = args.bundle
    version = parse_single_version(catalog, bundle_name, args.version)
    print_sha = args.sha
    bundle_list(catalog, bundle_name, version, print_sha=print_sha)


def subcmd_bundle_update(config, args):
    catalog = get_catalog(config, args)

    flavors = None
    if args.flavors is not None:
        with open(args.flavors) as f:
            flavors_dict = json.load(f)
            flavors = ZincFlavorSpec.from_dict(flavors_dict)

    bundle_name = args.bundle
    path = args.path
    force = args.force
    skip_master_archive = args.skip_master_archive

    bundle_update(catalog, bundle_name, path, flavors=flavors, force=force,
                  skip_master_archive=skip_master_archive)


def subcmd_bundle_clone(config, args):
    catalog = get_catalog(config, args)
    bundle_name = args.bundle
    bundle_id = helpers.make_bundle_id(catalog.id, bundle_name)
    version = parse_single_version(catalog, bundle_name, args.version)
    flavor = args.flavor
    root_path = args.path

    if args.no_versions:
        bundle_dir_name = bundle_id
    else:
        bundle_dir_name = None

    client.clone_bundle(catalog, bundle_name, version, root_path=root_path,
                        bundle_dir_name=bundle_dir_name, flavor=flavor)

    if args.no_versions:
        manifest_name = '%s.json' % (bundle_id)
    else:
        manifest_name = '%s.json' % (helpers.make_bundle_descriptor(bundle_id, version))

    manifest_dest_path = os.path.join(root_path, manifest_name)
    manifest_src_path = catalog.path_helper.path_for_manifest_for_bundle_version(bundle_name, version)
    _dump_json(catalog, manifest_src_path, dest_path=manifest_dest_path)


def subcmd_bundle_delete(config, args):
    catalog = get_catalog(config, args)
    bundle_name = args.bundle
    versions = parse_multi_versions(catalog, bundle_name, args.version)
    dry_run = args.dry_run
    bundle_delete(catalog, bundle_name, versions, dry_run=dry_run)


def subcmd_bundle_verify(config, args):
    catalog = get_catalog(config, args)
    bundle_name = args.bundle
    version = parse_single_version(catalog, bundle_name, args.version)
    client.verify_bundle(catalog, bundle_name=bundle_name, version=version)


def subcmd_distro_update(config, args):

    catalog = get_catalog(config, args)
    bundle_name = args.bundle
    distro_name = args.distro
    version = parse_single_version(catalog, bundle_name, args.version)
    save_previous = not args.no_prev_distro
    distro_update(catalog, bundle_name, distro_name, version,
                  save_previous=save_previous)


def subcmd_distro_delete(config, args):
    catalog = get_catalog(config, args)
    bundle_name = args.bundle
    distro_name = args.distro
    distro_delete(catalog, distro_name, bundle_name)


def subcmd_catalog_verify(config, args):
    catalog = get_catalog(config, args)
    should_lock = args.lock
    results = client.verify_catalog(catalog, should_lock=should_lock)
    if len(results) == 0:
        print 'All ok!'
    else:
        for r in results:
            print r


def _dump_json(catalog, subpath, dest_path=None, should_decompress=True):

    subpath_gz = helpers.append_file_extension_for_format(subpath, Formats.GZ)
    data = catalog._read(subpath_gz)  # TODO: fix private method references
    if should_decompress:
        out = utils.gunzip_bytes(data)
    else:
        out = data

    if dest_path is not None:
        with open(dest_path, 'w+b') as f:
            f.write(out)
        log.info('Wrote %s' % (dest_path))
    else:
        print out


def subcmd_dump_index(config, args):

    # TODO: this should not load the catalog - it should just pull the file from
    # the storage

    catalog = get_catalog(config, args)

    subpath = catalog.path_helper.path_for_index()

    if args.remote_name:
        dest_path = os.path.split(subpath)[-1]
    else:
        dest_path = None

    _dump_json(catalog, subpath, should_decompress=not args.no_decompress,
               dest_path=dest_path)


def subcmd_dump_manifest(config, args):

    # TODO: this should not load the catalog - it should just pull the file from
    # the storage

    catalog = get_catalog(config, args)
    bundle_name = args.bundle
    version = parse_single_version(catalog, bundle_name, args.version)

    subpath = catalog.path_helper.path_for_manifest_for_bundle_version(bundle_name, version)

    if args.remote_name:
        dest_path = os.path.split(subpath)[-1]
    else:
        dest_path = None

    _dump_json(catalog, subpath, should_decompress=not args.no_decompress,
               dest_path=dest_path)


### Main #####################################################################

def main():
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-C', '--config', default=DEFAULT_CONFIG_PATH,
            help='Config file path. Defaults to \'%s\'.' % (DEFAULT_CONFIG_PATH))

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
    add_version_arg = lambda parser, required=True: parser.add_argument(
        '-v', '--version', required=required, help='Version.')
    add_timeout_arg = lambda parser, required=False: parser.add_argument(
        '--timeout', required=required, default=None,
        help='Timeout for acquiring catalog locks. 0 = wait forever.')
    add_remote_name_arg = lambda parser, required=False: parser.add_argument(
            '-O', '--remote-name', required=required, default=False,
                action='store_true', help='Write to file using remote name \
                instead of stdout.')
    add_no_decompress_arg = lambda parser, required=False: parser.add_argument(
            '--no-decompress', required=required, default=False,
            action='store_true', help='Don\'t decompress file after downloading.')

    # catalog:create
    parser_catalog_create = subparsers.add_parser(
            'catalog:create', help='catalog:create help')
    parser_catalog_create.add_argument('catalog_id')
    parser_catalog_create.add_argument('-s', '--storage',
                                       help='Storage descriptor. Defaults to "file://."',
                                       default='.')
    parser_catalog_create.set_defaults(func=subcmd_catalog_create)

    # catalog:clean
    parser_catalog_clean = subparsers.add_parser(
            'catalog:clean', help='catalog:clean help')
    add_catalog_arg(parser_catalog_clean)
    add_timeout_arg(parser_catalog_clean)
    parser_catalog_clean.add_argument(
            '-f', '--force', default=False, action='store_true',
            help='This command does a dry run by default. Specifying this flag '
            'will cause files to actually be removed.')
    parser_catalog_clean.set_defaults(func=subcmd_catalog_clean)

    # catalog:verify
    parser_catalog_verify = subparsers.add_parser(
            'catalog:verify', help='catalog:verify help')
    add_catalog_arg(parser_catalog_verify)
    parser_catalog_verify.add_argument(
            '--lock', default=False, action='store_true',
            help='Lock the catalog while verifying.')
    parser_catalog_verify.set_defaults(func=subcmd_catalog_verify)

    # catalog:list
    parser_catalog_list = subparsers.add_parser(
            'catalog:list', help='List contents of catalog')
    add_catalog_arg(parser_catalog_list)
    add_distro_arg(parser_catalog_list, required=False)
    parser_catalog_list.add_argument(
            '--no-versions', default=False, action='store_true',
            help='Omit version information for bundles.')
    parser_catalog_list.set_defaults(func=subcmd_catalog_list)

    # bundle:list
    parser_bundle_list = subparsers.add_parser(
            'bundle:list', help='List contents of a bundle')
    add_catalog_arg(parser_bundle_list)
    add_bundle_arg(parser_bundle_list)
    add_version_arg(parser_bundle_list)
    parser_bundle_list.add_argument(
            '--sha', default=False, action='store_true',
            help='Print file SHA hash.')
    parser_bundle_list.set_defaults(func=subcmd_bundle_list)

    # bundle:update
    parser_bundle_update = subparsers.add_parser(
            'bundle:update', help='bundle:update help')
    add_catalog_arg(parser_bundle_update)
    add_timeout_arg(parser_bundle_update)
    add_bundle_arg(parser_bundle_update)
    parser_bundle_update.add_argument(
            '-p', '--path', required=True,
            help='Path to files for this bundle.')
    parser_bundle_update.add_argument(
            '--flavors', help='Flavor spec path. Should be JSON.')
    parser_bundle_update.add_argument(
            '-f', '--force', default=False, action='store_true',
            help='Update bundle even if no files changed.')

    parser_bundle_update_master_archive_group = parser_bundle_update.add_mutually_exclusive_group()
    parser_bundle_update_master_archive_group.add_argument(
            '--skip-master-archive', default=True, action='store_true', dest='skip_master_archive',
            help='Skips creating master archive if flavors are specified. This is the default behavior.')
    parser_bundle_update_master_archive_group.add_argument(
            '--include-master-archive', default=False, action='store_true', dest='skip_master_archive',
            help='Also creates master archive if flavors are specified.')

    parser_bundle_update.set_defaults(func=subcmd_bundle_update)

    # bundle:clone
    parser_bundle_clone = subparsers.add_parser(
            'bundle:clone', help='Clones a bundle to a local directory.')
    add_catalog_arg(parser_bundle_clone)
    add_bundle_arg(parser_bundle_clone)
    add_version_arg(parser_bundle_clone)
    parser_bundle_clone.add_argument(
            '-p', '--path', required=False, default='.',
            help='Destination path for bundle clone.')
    parser_bundle_clone.add_argument(
            '--flavor', help='Name of flavor.')
    parser_bundle_clone.add_argument(
            '--include-manifest', required=False, action='store_true',
            help='Also dump bundle manifest.')
    parser_bundle_clone.add_argument(
            '--no-versions', default=False, action='store_true',
            help='Omit version when writing bundle file names.')
    parser_bundle_clone.set_defaults(func=subcmd_bundle_clone)

    # bundle:delete
    parser_bundle_delete = subparsers.add_parser(
            'bundle:delete', help='bundle:delete help')
    add_catalog_arg(parser_bundle_delete)
    add_timeout_arg(parser_bundle_delete)
    add_bundle_arg(parser_bundle_delete)
    add_version_arg(parser_bundle_delete)
    parser_bundle_delete.add_argument(
            '-n', '--dry-run', default=False, action='store_true',
            help='Dry run. Don\' actually delete anything.')
    parser_bundle_delete.set_defaults(func=subcmd_bundle_delete)

    # bundle:verify
    parser_bundle_verify = subparsers.add_parser(
            'bundle:verify', help = 'Verify all contents of a bundle.')
    add_catalog_arg(parser_bundle_verify)
    add_bundle_arg(parser_bundle_verify)
    add_version_arg(parser_bundle_verify)
    # TODO: allow for version OR distro
    parser_bundle_verify.set_defaults(func=subcmd_bundle_verify)

    # distro:update
    parser_distro_update = subparsers.add_parser(
            'distro:update', help='distro:update help')
    add_catalog_arg(parser_distro_update)
    add_timeout_arg(parser_distro_update)
    add_bundle_arg(parser_distro_update)
    add_version_arg(parser_distro_update)
    add_distro_arg(parser_distro_update)
    parser_distro_update.add_argument('--no-prev-distro', default=False, action='store_true',
                                      help='Do not preserve previous version for distro.')
    parser_distro_update.set_defaults(func=subcmd_distro_update)

    # distro:delete
    parser_distro_delete = subparsers.add_parser(
            'distro:delete', help='distro:delete help')
    add_catalog_arg(parser_distro_delete)
    add_timeout_arg(parser_distro_delete)
    add_bundle_arg(parser_distro_delete)
    add_distro_arg(parser_distro_delete)
    parser_distro_delete.set_defaults(func=subcmd_distro_delete)

    # dump:index
    parser_dump_index = subparsers.add_parser(
            'dump:index', help='Dump catalog index file (catalog.json).')
    add_catalog_arg(parser_dump_index)
    add_remote_name_arg(parser_dump_index)
    add_no_decompress_arg(parser_dump_index)
    parser_dump_index.set_defaults(func=subcmd_dump_index)

    # dump:manifest
    parser_dump_manifest = subparsers.add_parser(
            'dump:manifest', help='Dump a manifest file from a catalog.')
    add_catalog_arg(parser_dump_manifest)
    add_bundle_arg(parser_dump_manifest)
    add_version_arg(parser_dump_manifest)
    add_remote_name_arg(parser_dump_manifest)
    add_no_decompress_arg(parser_dump_manifest)
    parser_dump_manifest.set_defaults(func=subcmd_dump_manifest)

    args = parser.parse_args()
    config = load_config(args.config)
    set_loglevel(args)
    args.func(config, args)


if __name__ == "__main__":
    main()
