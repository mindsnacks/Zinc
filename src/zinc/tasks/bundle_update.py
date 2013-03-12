import os.path

from zinc.utils import *
from zinc.helpers import *
from zinc.models import ZincFileList

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

    def _import_files(self, src_dir, flavor_spec=None):

        filelist = ZincFileList()

        for root, dirs, files in os.walk(src_dir):
            for f in files:
                if f in IGNORE: continue # TODO: real ignore
                full_path = os.path.join(root, f)
                rel_dir = root[len(src_dir)+1:]
                rel_path = os.path.join(rel_dir, f)

                file_info = self.catalog.import_path(full_path)
                if file_info is not None:
                    filelist.add_file(rel_path, file_info['sha'])
                    filelist.add_format_for_file(
                            rel_path, file_info['format'], file_info['size'])

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

        return self.catalog.update_bundle(
                self.bundle_name, filelist, force=self.force,
                skip_master_archive=self.skip_master_archive)

