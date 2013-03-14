import os
from shutil import copyfile
import logging

from zinc.formats import Formats
from zinc.utils import *
from zinc.helpers import *

log = logging.getLogger(__name__)

## TODO: this is broken and needs to be tested too

class ZincBundleCloneTask(object):

    def __init__(self,
            catalog=None,
            bundle_name=None,
            version=None,
            output_path=None,
            flavor=None):

        self.catalog = catalog
        self.bundle_name = bundle_name
        self.version = version
        self.output_path = output_path
        self.flavor = flavor

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, val):
        if val is not None and not isinstance(val, int):
            raise Exception("Version must be an integer")
        self._version = val

    def run(self):

        assert self.catalog
        assert self.bundle_name
        assert self.version
        assert self.output_path

        manifest = self.catalog.manifest_for_bundle(
                self.bundle_name, self.version)

        if manifest is None:
            raise Exception("manifest not found: %s-%d" %
                    (self.bundle_name, self.version))

        if self.flavor is not None and self.flavor not in manifest.flavors:
            raise Exception("manifest does not contain flavor '%s'" %
                    (self.flavor))

        all_files = manifest.get_all_files(flavor=self.flavor)

        makedirs(self.output_path)
        bundle_id = make_bundle_id(
                self.catalog.id, self.bundle_name)
        bundle_descriptor = make_bundle_descriptor(
                bundle_id, self.version, flavor=self.flavor)
        root_dir = os.path.join(self.output_path, bundle_descriptor)

        for file in all_files:
            dst_path = os.path.join(root_dir, file)

            formats = manifest.formats_for_file(file)
            sha = manifest.sha_for_file(file)

            makedirs(os.path.dirname(dst_path))

            if formats.get(Formats.RAW) is not None:
                src_path = self.catalog._path_for_file_with_sha(sha)
                copyfile(src_path, dst_path)
            elif formats.get(Formats.GZ) is not None:
                src_path = self.catalog._path_for_file_with_sha(sha, ext='gz')
                gunzip(src_path, dst_path)

            log.info("Exported %s --> %s" % (src_path, dst_path))

        log.info("Exported %d files to '%s'" % (len(all_files), root_dir))

