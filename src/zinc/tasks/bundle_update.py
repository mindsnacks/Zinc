import os.path
import tarfile

from zinc.utils import *
from zinc.helpers import *
from zinc.models import ZincManifest
from zinc.catalog import ZincCatalogPathHelper

# TODO: real ignore system
IGNORE = ['.DS_Store']

class ZincBundleUpdateTask(object):

    def __init__(self, 
            catalog=None,
            bundle_name=None, 
            src_dir=None,
            flavor_spec=None, 
            force=False, 
            skip_master_archive=False):

        self.catalog = catalog
        self.bundle_name = bundle_name
        self.src_dir = src_dir
        self.flavor_spec = flavor_spec
        self.force = force
        self.skip_master_archive = skip_master_archive

    @property
    def src_dir(self):
        return self._src_dir

    @src_dir.setter
    def src_dir(self, val):
        if val is not None: 
            val = canonical_path(val)
        self._src_dir = val

    def _generate_manifest(self, version, flavor_spec=None):
        """Create a new temporary manifest."""
        new_manifest = ZincManifest(
                self.catalog.index.id, self.bundle_name, version)

        # Process all the paths and add them to the manifest
        for root, dirs, files in os.walk(self.src_dir):
            for f in files:
                if f in IGNORE: continue # TODO: real ignore
                full_path = os.path.join(root, f)
                rel_dir = root[len(self.src_dir)+1:]
                rel_path = os.path.join(rel_dir, f)
                sha = sha1_for_path(full_path)
                new_manifest.add_file(rel_path, sha)
        return new_manifest

    def _import_files_for_manifest(self, manifest, flavor_spec=None):

        for file in manifest.files.keys():
            full_path = os.path.join(self.src_dir, file)
            
            (catalog_path, size) = self.catalog._import_path(full_path)
            if catalog_path[-3:] == '.gz':
                format = 'gz'
            else:
                format = 'raw'
            manifest.add_format_for_file(file, format, size)

            if flavor_spec is not None:
                for flavor in flavor_spec.flavors:
                    filter = flavor_spec.filter_for_flavor(flavor)
                    if filter.match(full_path):
                        manifest.add_flavor_for_file(file, flavor)

        if len(manifest.files) > 1:

            if flavor_spec is None or not self.skip_master_archive:
                #TODO: fix
                master_tar_file_path = self.catalog.path_in_catalog(
                        ZincCatalogPathHelper().path_for_archive_for_bundle_version(
                            self.bundle_name, manifest.version))
                master_tar = tarfile.open(master_tar_file_path, 'w')
            else:
                master_tar = None

            flavor_tar_files = dict()
            if flavor_spec is not None:
                for flavor in flavor_spec.flavors:
                    flavor_files = manifest.get_all_files(flavor=flavor)
                    if len(flavor_files) > 1:
                        #TODO: fix
                        tar_file_path = self.catalog.path_in_catalog(
                                ZincCatalogPathHelper().path_for_archive_for_bundle_version(
                                    self.bundle_name, manifest.version, flavor))
                        tar = tarfile.open(tar_file_path, 'w')
                        flavor_tar_files[flavor] = tar
    
            for file in manifest.files.keys():
                full_path = os.path.join(self.src_dir, file)
                sha = manifest.sha_for_file(file)

                ext = 'gz' if manifest.formats_for_file(file).get('gz') else None

                #TODO: fix
                catalog_path = self.catalog.path_in_catalog(
                        ZincCatalogPathHelper().path_for_file_with_sha(
                            sha, ext=ext))

                #TODO: fix
                makedirs(os.path.dirname(catalog_path))

                member_name = os.path.basename(catalog_path)

                if master_tar is not None:
                    master_tar.add(catalog_path, member_name)

                if flavor_spec is not None:
                    for flavor in flavor_spec.flavors:
                        filter = flavor_spec.filter_for_flavor(flavor)
                        if filter.match(full_path):
                            manifest.add_flavor_for_file(file, flavor)
                            flavor_tar = flavor_tar_files.get(flavor)
                            if flavor_tar is not None:
                                tar = flavor_tar.add(
                                        catalog_path, member_name)
            
            if master_tar is not None:
                master_tar.close() 
            
            for k, v in flavor_tar_files.iteritems():
                v.close()

    def run(self):

        assert self.catalog
        assert self.bundle_name
        assert self.src_dir

        version = self.catalog.index.next_version_for_bundle(self.bundle_name)

        manifest = self.catalog.manifest_for_bundle(self.bundle_name)
        new_manifest = self._generate_manifest(version)

        should_create_new_version = \
                self.force or \
                manifest is None \
                or not new_manifest.files_are_equivalent(manifest)

        if should_create_new_version:
            manifest = new_manifest
            self._import_files_for_manifest(manifest, self.flavor_spec)

            self.catalog._write_manifest(manifest)
            self.catalog.index.add_version_for_bundle(self.bundle_name, version)
            self.catalog.save()

        return manifest

