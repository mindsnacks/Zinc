import os
import logging
import tempfile

import zinc.utils as utils
from zinc.models import ZincFileList, ZincManifest
from zinc.archives import build_archive_with_manifest

log = logging.getLogger(__name__)

## TODO: real ignore system
IGNORE = ['.DS_Store']


class ZincBundleUpdateTask(object):

    def __init__(self,
                 catalog=None,
                 bundle_name=None,
                 src_dir=None,
                 flavor_spec=None,
                 force=False,
                 skip_master_archive=True):

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
            val = utils.canonical_path(val)
        self._src_dir = val

    @classmethod
    def _build_archive(catalog, manifest, src_dir, flavor=None):

        archive_filename = catalog.path_helper.archive_name(manifest.bundle_name,
                                                            manifest.version,
                                                            flavor=flavor)
        archive_path = os.path.join(tempfile.mkdtemp(), archive_filename)
        build_archive_with_manifest(manifest, src_dir, archive_path, flavor=flavor)
        return archive_path

    def _import_files(self, src_dir, flavor_spec=None):

        filelist = ZincFileList()

        for root, dirs, files in os.walk(src_dir):
            for f in files:
                if f in IGNORE:
                    continue  # TODO: real ignore
                full_path = os.path.join(root, f)
                rel_dir = root[len(src_dir) + 1:]
                rel_path = os.path.join(rel_dir, f)

                file_info = self.catalog.import_path(full_path)
                if file_info is not None:
                    filelist.add_file(rel_path, file_info['sha'])
                    filelist.add_format_for_file(rel_path, file_info['format'], file_info['size'])

                    if flavor_spec is not None:
                        for flavor in flavor_spec.flavors:
                            filter = flavor_spec.filter_for_flavor(flavor)
                            if filter.match(full_path):
                                filelist.add_flavor_for_file(rel_path, flavor)
                else:
                    # TODO: better error
                    raise Exception("we broke")

        return filelist

    def run(self):

        assert self.catalog
        assert self.bundle_name
        assert self.src_dir

        filelist = self._import_files(self.src_dir, self.flavor_spec)

        ## Check if it matches the newest version
        ## TODO: optionally check it if matches any existing versions?

        if not self.force:
            existing_manifest = self.catalog.manifest_for_bundle(self.bundle_name)
            if existing_manifest is not None \
               and existing_manifest.files.contents_are_equalivalent(filelist):
                log.info("Found existing version with same contents.")
                return existing_manifest

        ## Build manifest

        version = self.catalog._reserve_version_for_bundle(self.bundle_name)
        new_manifest = ZincManifest(self.catalog.id, self.bundle_name, version)
        new_manifest.files = filelist.clone(mutable=True)
        # TODO move into setter?

        ## Handle archives

        should_create_archives = len(filelist) > 1
        if should_create_archives:

            archive_flavors = list()

            # should create master archive?
            if len(new_manifest.flavors) == 0 or not self.skip_master_archive:
                # None is the appropriate flavor for the master archive
                archive_flavors.append(None)

            # should create archives for flavors?
            if new_manifest.flavors is not None:
                archive_flavors.extend(new_manifest.flavors)

            for flavor in archive_flavors:
                tmp_tar_path = self._build_archive(
                    self.catalog, new_manifest, self.src_dir, flavor=flavor)
                self.catalog._write_archive(
                    self.bundle_name, new_manifest.version,
                    tmp_tar_path, flavor=flavor)

                # TODO: remove remove?
                os.remove(tmp_tar_path)

        self.catalog.update_bundle(new_manifest)

        return new_manifest
