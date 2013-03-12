
from zinc.utils import *
from zinc.helpers import *
from zinc.defaults import defaults

################################################################################

# TODO: rename to CatalogPathHelper?
class ZincCatalogPathHelper(object):

    def __init__(self, format='1'):
        if format != defaults['zinc_format']:
            raise Exception("Incompatible format %s" % (format))
        self._format = format

    @property
    def format(self):
        return self._format

    @property
    def manifests_dir(self):
        return "manifests"

    @property
    def archives_dir(self):
        return "archives"

    @property
    def objects_dir(self):
        return "objects"

    def path_for_index(self):
        return defaults['catalog_index_name']

    def manifest_name(self, bundle_name, version):
        return "%s-%d.json" % (bundle_name, version)

    def path_for_manifest_for_bundle_version(self, bundle_name, version):
        manifest_filename = self.manifest_name(bundle_name, version)
        manifest_path = os.path.join(self.manifests_dir, manifest_filename)
        return manifest_path

    def path_for_manifest(self, manifest):
        return self.path_for_manifest_for_bundle_version(
                manifest.bundle_name, manifest.version)

    def path_for_file_with_sha(self, sha, ext=None, format=None):

        if ext is not None and format is not None:
            raise Exception(
                    "Should specify either `ext` or `format`, not both.")

        if format is not None:
            ext = file_extension_for_format(format)
        subdir = os.path.join(self.objects_dir, sha[0:2], sha[2:4])
        file = sha
        if ext is not None:
            file = file + '.' + ext
        return os.path.join(subdir, file)

    def archive_name(self, bundle_name, version, flavor=None):
        if flavor is None:
            return "%s-%d.tar" % (bundle_name, version)
        else:
            return "%s-%d~%s.tar" % (bundle_name, version, flavor)

    def path_for_archive_for_bundle_version(
            self, bundle_name, version, flavor=None):
        archive_filename = self.archive_name(bundle_name, version, flavor=flavor)
        archive_path = os.path.join(self.archives_dir, archive_filename)
        return archive_path


################################################################################

class ZincAbstractCatalog(object):

    def get_index(self):
        """
        Returns an *immutable* copy of the catalog index.
        """
        raise NotImplementedError()

    def get_manifest(self, bundle_name, version):
        """
        Returns an *immutable* copy of the manifest for the specified
        `bundle_name` and version`.
        """
        raise NotImplementedError()

    def update_bundle(self, bundle_name, filelist,
            skip_master_archive=False, force=False):
        raise NotImplementedError()

    # special
    def import_path(self, src_path):
        raise NotImplementedError()

    def delete_bundle_version(self, bundle_name, version):
        raise NotImplementedError()

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        raise NotImplementedError()

    def delete_distribution(self, distribution_name, bundle_name):
        raise NotImplementedError()

    def verify(self):
        raise NotImplementedError()

    def clean(self):
        raise NotImplementedError()

    ### Non-abstract methods

    def manifest_for_bundle(self, bundle_name, version=None):
        """
        Get a manifest for bundle. If version is not specified, it gets the
        manifest with the highest version number.
        """
        index = self.get_index()
        all_versions = index.versions_for_bundle(bundle_name)
        if version is None and len(all_versions) > 0:
            version = all_versions[-1]
        elif version not in all_versions:
            return None # throw exception?
        return self.get_manifest(bundle_name, version)

    def manifest_for_bundle_descriptor(self, bundle_descriptor):
        """
        Convenience method to get a manifest by bundle_descriptor.
        """
        return self.manifest_for_bundle(
            bundle_id_from_bundle_descriptor(bundle_descriptor),
            bundle_version_from_bundle_descriptor(bundle_descriptor))

    def bundle_descriptors(self):
        bundle_descriptors = []
        index = self.get_index()
        for bundle_name in index.bundle_names():
            for version in index.versions_for_bundle(bundle_name):
                bundle_descriptors.append("%s-%d" % (bundle_name, version))
                manifest = self.manifest_for_bundle(bundle_name, version)
                for flavor in manifest.flavors:
                    bundle_descriptors.append("%s-%d~%s" %
                            (bundle_name, version, flavor))
        return bundle_descriptors

