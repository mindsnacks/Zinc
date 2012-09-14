import logging
import argparse
import os
import json
import tarfile


from zinc.defaults import defaults
from zinc.models import ZincFlavorSpec

from zinc.tasks.bundle_clone import ZincBundleCloneTask
#from zinc.backends.filesystem import load_catalog_at_path

from zinc.backends.filesystem import FileSystemIndexBackend, FileSystemStorageBackend
from zinc.backends.aws import S3StorageBackend

from zinc.models import load_config, ZincCatalog
from zinc.utils import makedirs, canonical_path, gzip_path

logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s')

###

def load_catalog_at_path(path):
    makedirs(path)
    
    config_path = os.path.join(path, 'config.json')
    config = load_config(config_path)

    storage_backend_type = config.storage_backend
    if storage_backend_type == 'S3':
        aws_key = config._dict['aws_key']
        aws_secret = config._dict['aws_secret']
        bucket = config._dict['s3_bucket']
        storage_backend = S3StorageBackend(
                aws_key=aws_key, aws_secret=aws_secret, bucket_name=bucket)
    else:
        storage_backend = FileSystemStorageBackend(path)


    index_backend = FileSystemIndexBackend(path)
    return ZincCatalog(index_backend, storage_backend)






### Commands #################################################################

def catalog_create(args):
    dest = args.catalog_path
    if dest is None:
        dest = './%s' % (args.catalog_id)
    create_catalog_at_path(dest, args.catalog_id)

def catalog_clean(args):
    catalog = load_catalog_at_path(args.catalog_path)
    catalog.clean(dry_run=not args.force)

def catalog_list(args):
    catalog = load_catalog_at_path(args.catalog_path)
    for bundle_name in catalog.bundle_names():
        distros = catalog.index.distributions_for_bundle_by_version(bundle_name)
        versions = catalog.versions_for_bundle(bundle_name)
        version_strings = list()
        for version in versions:
            version_string = str(version)
            if distros.get(version) is not None:
                distro_string = "(%s)" % (", ".join(distros.get(version)))
                version_string += '=' + distro_string
            version_strings.append(version_string)

        final_string =  "[%s]" %(", ".join(version_strings))
        print "%s %s" % (bundle_name, final_string)

def bundle_list(args):
    catalog = load_catalog_at_path(args.catalog_path)
    bundle_name = args.bundle_name
    version = int(args.version)
    manifest = catalog.manifest_for_bundle(bundle_name, version=version)
    all_files = sorted(manifest.get_all_files())
    for f in all_files:
        if args.sha:
            print f, 'sha=%s' % (manifest.sha_for_file(f))
        else:
            print f


def bundle_update(args):
    flavors = None
    if args.flavors is not None:
        with open(args.flavors) as f:
            flavors_dict = json.load(f)
            flavors = ZincFlavorSpec.from_dict(flavors_dict)

    catalog = load_catalog_at_path(args.catalog_path)
    bundle_name = args.bundle_name
    path = args.path
    force = args.force
    manifest = catalog.create_bundle_version(
            bundle_name, path, flavor_spec=flavors, force=force)
    print "Updated %s v%d" % (manifest.bundle_name, manifest.version)

def bundle_clone(args):
    catalog = load_catalog_at_path(args.catalog_path)

    task = ZincBundleCloneTask()
    task.catalog = catalog
    task.bundle_name = args.bundle_name
    task.version = int(args.version)
    task.flavor = args.flavor
    task.output_path = args.path

    task.run()

def bundle_delete(args):
    bundle_name = args.bundle_name
    version = args.version
    catalog = load_catalog_at_path(args.catalog_path)
    if version == 'all':
        versions_to_delete = catalog.versions_for_bundle(bundle_name)
    else:
        versions_to_delete = [version]
        for v in versions_to_delete:
            catalog.delete_bundle_version(bundle_name, int(v))

def distro_update(args):
    catalog = load_catalog_at_path(args.catalog_path)
    bundle_name = args.bundle_name
    distro_name = args.distro_name
    bundle_version = args.version
    if bundle_version != "latest":
        bundle_version = int(bundle_version)
    catalog.update_distribution(
            distro_name, bundle_name, bundle_version)

def distro_delete(args):
    catalog = load_catalog_at_path(args.catalog_path)
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
            help='Destination path. Defaults to "./<catalog_id>"')
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

    # catalog:list
    parser_catalog_list = subparsers.add_parser('catalog:list', 
            help='List contents of catalog')
    parser_catalog_list.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_catalog_list.set_defaults(func=catalog_list)

    # bundle:list
    parser_bundle_list = subparsers.add_parser('bundle:list', 
            help='List contents of a bundle')
    parser_bundle_list.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_bundle_list.add_argument('--sha', default=False, action='store_true', 
            help='Print file SHA hash.')
    parser_bundle_list.add_argument('bundle_name',
            help='Name of the bundle. Must not contain a period (.).')
    parser_bundle_list.add_argument('version',
            help='Version number or "latest".')
    parser_bundle_list.set_defaults(func=bundle_list)

    # bundle:update
    parser_bundle_update = subparsers.add_parser('bundle:update', help='bundle:update help')
    parser_bundle_update.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_bundle_update.add_argument('--flavors', 
            help='Flavor spec path. Should be JSON.')
    parser_bundle_update.add_argument('-f', '--force', default=False, action='store_true', 
            help='Update bundle even if no files changed.')
    parser_bundle_update.add_argument('bundle_name',
            help='Name of the bundle. Must not contain a period (.).')
    parser_bundle_update.add_argument('path',
            help='Path to files for this bundle.')
    parser_bundle_update.set_defaults(func=bundle_update)

    # bundle:clone
    parser_bundle_clone = subparsers.add_parser('bundle:clone',
            help='Clones a bundle to a local directory.')
    parser_bundle_clone.add_argument('-c', '--catalog_path', default='.',
            help='Catalog path. Defaults to "."')
    parser_bundle_clone.add_argument('--flavor', help='Name of flavor.')
    parser_bundle_clone.add_argument('bundle_name',
            help='Name of the bundle. Must not contain a period (.).')
    parser_bundle_clone.add_argument('version',
            help='Version number or "latest".')
    parser_bundle_clone.add_argument('path',
            help='Destination path for bundle clone.')
    parser_bundle_clone.set_defaults(func=bundle_clone)

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

if __name__ == "__main__":
    main()

