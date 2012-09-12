import os
import tarfile
import tempfile
import shutil

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
            force=False,
            create_archives=True):

        self.catalog = catalog
        self.bundle_name = bundle_name
        self.src_dir =  canonical_path(src_dir)
        self.flavor_spec = flavor_spec
        self.force = force
        self.temp_dir = tempfile.mkdtemp()
        self.create_archives = create_archives

        self._master_tar = None
        self._flavor_tars = None

    def _next_version_for_bundle(self, bundle_name):
        versions = self.catalog.versions_for_bundle(bundle_name)
        if len(versions) == 0:
            return 1
        return versions[-1] + 1

    def _generate_manifest(self):
        """Create a new temporary manifest."""
        new_manifest = ZincManifest(
                self.catalog.index.id, self.bundle_name, self._version)

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
        return os.path.join(self.temp_dir, filename)

    def _temp_archive_path(self, flavor=None):
        tar_filename = self.catalog._archive_filename(
                self.bundle_name, self._version, flavor=flavor)
        tar_path = self._temp_path(tar_filename)
        return tar_path

    def _create_tars(self, manifest):

        if not self.create_archives: return

        master_tar_path = self._temp_archive_path()
        self._master_tar = tarfile.open(master_tar_path, 'w')

        if self.flavor_spec is not None:
            self._flavor_tars = dict()
            for flavor in self.flavor_spec.flavors:
                tar_path = self._temp_archive_path(flavor=flavor)
                tar = tarfile.open(tar_path, 'w')
                self._flavor_tars[flavor] = tar

    def _finish_tars(self):
        if self._master_tar is not None:
            self._master_tar.close()
            self._master_tar = None

        if self._flavor_tars is not None:
            for k, v in self._flavor_tars.iteritems():
                v.close()
            self._flavor_tars = None

    def _add_file_to_archive(self, path, flavor=None):

        if not self.create_archives: return

        if flavor is None:
            tar = self._master_tar
        else:
            tar = self._flavor_tars[flavor]

        tar.add(path, os.path.basename(path))

    def _import_archives(self):

        if not self.create_archives: return

        master_tar_path = self._temp_archive_path()
        self.catalog._import_archive(master_tar_path,
                self.bundle_name, self._version)

        if self.flavor_spec is not None:
            for flavor in self.flavor_spec.flavors:
                tar_path = self._temp_archive_path(flavor=flavor)
                self.catalog._import_archive(master_tar_path,
                        self.bundle_name, self._version, flavor=flavor)


    def _import_files_for_manifest(self, manifest):

        self._create_tars(manifest)
    
        for file in manifest.files.keys():
            full_path = os.path.join(self.src_dir, file)

            (catalog_path, size) = self.catalog._import_path(full_path)
            if catalog_path[-3:] == '.gz':
                format = 'gz'
            else:
                format = 'raw'
            manifest.add_format_for_file(file, format, size)

            self._add_file_to_archive(catalog_path)

            if self.flavor_spec is not None: 
                for flavor in self.flavor_spec.flavors:
                    filter = self.flavor_spec.filter_for_flavor(flavor)
                    if filter.match(full_path):
                        manifest.add_flavor_for_file(file, flavor)
                        self._add_file_to_archive(catalog_path, flavor=flavor)

        self._finish_tars()
        self._import_archives()
            
    def _cleanup(self):
        self._finish_tars()
        shutil.rmtree(self.temp_dir)

    def run(self):
        self._version = self._next_version_for_bundle(self.bundle_name)

        manifest = self.catalog.manifest_for_bundle(self.bundle_name)
        new_manifest = self._generate_manifest()

        should_create_new_version = \
                self.force or \
                manifest is None \
                or not new_manifest.files_are_equivalent(manifest)

        if should_create_new_version:
            manifest = new_manifest

            self._import_files_for_manifest(manifest)

            self.catalog._write_manifest(manifest)
            self.catalog.index.add_version_for_bundle(self.bundle_name, self._version)
            self.catalog.save()

        self._cleanup()

        return manifest

