import os.path
import tarfile
import tempfile
#tmp
import StringIO
import shutil

from zinc.utils import *
from zinc.helpers import *
from zinc.models import ZincManifest
from zinc.catalog import ZincCatalogPathHelper

# TODO: real ignore system
IGNORE = ['.DS_Store']

def build_archive(catalog_coordinator, manifest, flavor=None):

    archive_filename = archive_name(
            manifest.bundle_name, manifest.version, flavor=flavor)
    archive_path = os.path.join(
            tempfile.mkdtemp(), archive_filename)
   
    files = manifest.get_all_files(flavor=flavor)
    
    with tarfile.open(archive_path, 'w') as tar:
        for f in files:
            format, format_info = manifest.get_format_info_for_file(f)
            sha = manifest.sha_for_file(f)
            ext = file_extension_for_format(format)
           
            # TODO: remove StringIO
            file_data = StringIO.StringIO(
                    catalog_coordinator.get_fileobj(sha, ext=ext))

            tarinfo = tar.tarinfo()
            tarinfo.name = filename_with_ext(sha, ext)
            tarinfo.size = format_info['size']
            
            tar.addfile(tarinfo, file_data)

    return archive_path
    

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
        self.flavor_spec = flavor_spec
        self.force = force
        self.skip_master_archive = skip_master_archive

        self._src_dir = src_dir

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
                self.catalog.id, self.bundle_name, version)

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

        should_create_archives = len(manifest.files) > 1
        if should_create_archives:

            flavors = list()

            # create master archive?
            if flavor_spec is None or not self.skip_master_archive:
                # None is the appropriate flavor for the master archive
                flavors.append(None) 

            # create archives for flavors?
            if flavor_spec is not None:
                flavors.extend(flavor_spec.flavors)

            # create appropriate archives
            for flavor in flavors:
                tmp_tar_path = build_archive(
                        self.catalog._coordinator, manifest, flavor=flavor)
                # TODO: remove call to path_in_catalog
                catalog_tar_path = self.catalog.path_in_catalog(
                        ZincCatalogPathHelper().path_for_archive_for_bundle_version(
                            self.bundle_name, manifest.version, flavor))
                # TODO: remove copyfile
                shutil.copyfile(tmp_tar_path, catalog_tar_path)

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

