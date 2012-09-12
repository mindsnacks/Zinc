import os
import tarfile
import tempfile

from zinc.models import ZincManifest
from zinc.utils import sha1_for_path, canonical_path

# TODO: real ignore system
IGNORE = ['.DS_Store']

class ZincBundleCreateTask(object):

    def __init__(self, 
            catalog, 
            bundle_name, 
            src_dir,
            flavor_spec=None, 
            force=False):

        self.catalog = catalog
        self.bundle_name = bundle_name
        self.src_dir =  canonical_path(src_dir)
        self.flavor_spec = flavor_spec
        self.force = force
        self.tempdir = tempfile.mkdtemp()

    def _next_version_for_bundle(self, bundle_name):
        versions = self.catalog.versions_for_bundle(bundle_name)
        if len(versions) == 0:
            return 1
        return versions[-1] + 1

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

    def _temp_path(self, filename):
        return os.path.join(self.tempdir, filename)

    def _import_files_for_manifest(self, manifest, flavor_spec=None):

        flavor_tar_files = dict()
        if flavor_spec is not None:
            for flavor in flavor_spec.flavors:
                tar_filename = self.catalog._archive_filename(
                        bundle_name, manifest.version, flavor=flavor)
                tar_path = self._temp_path(tar_filename)
                tar = tarfile.open(tar_path, 'w')
                flavor_tar_files[flavor] = tar

        master_tar_filename = self.catalog._archive_filename(
                self.bundle_name, manifest.version)
        master_tar_path = self._temp_path(master_tar_filename)
        master_tar = tarfile.open(master_tar_path, 'w')

        for file in manifest.files.keys():
            full_path = os.path.join(self.src_dir, file)
            
            (catalog_path, size) = self.catalog._import_path(full_path)
            if catalog_path[-3:] == '.gz':
                format = 'gz'
            else:
                format = 'raw'
            manifest.add_format_for_file(file, format, size)
            
            master_tar.add(catalog_path, os.path.basename(catalog_path))

            if flavor_spec is not None:
                for flavor in flavor_spec.flavors:
                    filter = flavor_spec.filter_for_flavor(flavor)
                    if filter.match(full_path):
                        manifest.add_flavor_for_file(file, flavor)
                        tar = flavor_tar_files[flavor].add(
                                catalog_path, os.path.basename(catalog_path))

        master_tar.close()
        for k, v in flavor_tar_files.iteritems():
            v.close()

    def run(self):
        version = self._next_version_for_bundle(self.bundle_name)

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

