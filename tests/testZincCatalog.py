import os
import logging
import json

from zinc.models import ZincIndex, ZincManifest, ZincFlavorSpec
from zinc.catalog import ZincCatalogPathHelper
from zinc.defaults import defaults
from zinc.catalog import ZincCatalog
from zinc.storages import StorageBackend

from zinc.client import connect, create_bundle_version

import zinc.helpers as helpers
import zinc.utils as utils

from tests import *


# TODO: relocate
class StorageBackendTestCase(unittest.TestCase):

    def setUp(self):
        self.storage = StorageBackend()

    def test_get_raises(self):
        self.assertRaises(NotImplementedError, self.storage.get, 'foo')

    def test_get_meta_raises(self):
        self.assertRaises(NotImplementedError, self.storage.get_meta, 'foo')

    def test_put_raises(self):
        self.assertRaises(NotImplementedError, self.storage.put, 'foo', 'bar')


def create_catalog_at_path(path, id):
    service = connect('/')
    service.create_catalog(id=id, loc=path)
    catalog = service.get_catalog(loc=path)
    return catalog


class ZincCatalogPathHelperTestCase(unittest.TestCase):

    def setUp(self):
        self.pathHelper = ZincCatalogPathHelper()

    def test_config_dir(self):
        self.assertEquals(self.pathHelper.config_dir, "config")

    def test_config_flavors_spec_dir(self):
        self.assertEquals(self.pathHelper.config_flavorspec_dir, "config/flavorspecs")

    def test_path_for_flavor_spec_name(self):
        flavor_spec_name = "games"
        expected_path = "config/flavorspecs/%s.json" % flavor_spec_name
        actual_path = self.pathHelper.path_for_flavorspec_name(flavor_spec_name)
        self.assertEquals(expected_path, actual_path)


class ZincCatalogTestCase(TempDirTestCase):

    def setUp(self):
        super(ZincCatalogTestCase, self).setUp()

        self.catalog_dir = os.path.join(self.dir, "catalog")
        os.mkdir(self.catalog_dir)
        self.scratch_dir = os.path.join(self.dir, "scratch")
        os.mkdir(self.scratch_dir)
        logging.info("catalog: %s" % self.catalog_dir)
        logging.info("scratch: %s" % self.scratch_dir)

    def path_exists_in_catalog(self, subpath):
        fullpath = os.path.join(self.catalog_dir, subpath)
        return os.path.exists(fullpath)

    def test_catalog_create(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        self.assertTrue(catalog is not None)
        self.assertTrue(len(catalog.index.bundle_names()) == 0)
        self.assertTrue(catalog.format() == defaults['zinc_format'])

    def test_catalog_read_invalid_format(self):
        create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        index_path = os.path.join(self.catalog_dir, defaults['catalog_index_name'])
        index = ZincIndex.from_path(index_path)
        index._format = 2
        index.write(index_path)
        self.assertRaises(Exception, ZincCatalog, (self.catalog_dir))

    def test_catalog_import_file(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        f1 = create_random_file(self.scratch_dir)
        catalog.import_path(f1)

    def test_bundle_names_with_no_bundles(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        self.assertTrue(len(catalog.index.bundle_names()) == 0)

    def test_versions_for_nonexistant_bundle(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        versions = catalog.index.versions_for_bundle("meep")
        self.assertTrue(len(versions) == 0)

    def _build_test_catalog(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        create_bundle_version(catalog, "meep", self.scratch_dir)
        catalog._reload()  # TODO: fix/remove/something
        return catalog

    def test_create_bundle_version(self):
        catalog = self._build_test_catalog()
        self.assertTrue("meep" in catalog.get_index().bundle_names())
        self.assertTrue(1 in catalog.get_index().versions_for_bundle("meep"))
        manifest = catalog.manifest_for_bundle("meep", 1)
        self.assertTrue(manifest is not None)
        for (file, props) in manifest.files.items():
            sha = props['sha']
            formats = props['formats']
            for format in formats.keys():
                ext = None
                if format == 'gz':
                    ext = 'gz'
                object_path = ZincCatalogPathHelper().path_for_file_with_sha(sha, ext)
                self.assertTrue(self.path_exists_in_catalog(object_path))

    def test_bundle_name_in_manifest(self):
        catalog = self._build_test_catalog()
        bundle_name = "meep"
        manifest = catalog.manifest_for_bundle(bundle_name, 1)
        self.assertTrue(manifest.bundle_name == bundle_name)

    def test_create_bundle_with_subdirs(self):
        create_random_file(self.scratch_dir)
        one_dir = os.mkdir(os.path.join(self.scratch_dir, "one"))
        create_random_file(one_dir)
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_bundle_version(catalog, "meep", self.scratch_dir)

    def test_create_second_bundle_version(self):
        catalog = self._build_test_catalog()
        # add a file
        create_random_file(self.scratch_dir)
        create_bundle_version(catalog, "meep", self.scratch_dir)
        self.assertTrue(2 in catalog.get_index().versions_for_bundle("meep"))
        new_index = ZincIndex.from_path(os.path.join(catalog.path, defaults['catalog_index_name']))
        self.assertTrue(1 in new_index.versions_for_bundle("meep"))
        self.assertTrue(2 in new_index.versions_for_bundle("meep"))

    def test_create_duplicate_bundle_version_no_force(self):
        catalog = self._build_test_catalog()
        # add a file
        create_random_file(self.scratch_dir)
        # create first version
        manifest1 = create_bundle_version(catalog, "meep", self.scratch_dir)
        self.assertTrue(2 in catalog.get_index().versions_for_bundle("meep"))
        # attempt to create same version again
        manifest2 = create_bundle_version(catalog, "meep", self.scratch_dir)
        self.assertEquals(manifest1.version, manifest2.version)

    def test_create_duplicate_bundle_version_with_force(self):
        catalog = self._build_test_catalog()
        # add a file
        create_random_file(self.scratch_dir)
        # create first version
        manifest1 = create_bundle_version(catalog, "meep", self.scratch_dir)
        self.assertTrue(2 in catalog.get_index().versions_for_bundle("meep"))
        # attempt to create same version again, with force
        manifest2 = create_bundle_version(catalog, "meep", self.scratch_dir,
                                          force=True)
        self.assertNotEquals(manifest1.version, manifest2.version)

    def test_create_identical_bundle_version(self):
        catalog = self._build_test_catalog()
        create_bundle_version(catalog, "meep", self.scratch_dir)
        self.assertEquals(len(catalog.get_index().versions_for_bundle("meep")), 1)

    def test_path_for_manifest_with_name_version(self):
        catalog = self._build_test_catalog()
        manifest = ZincManifest(catalog.index.id, 'zoo', 1)
        path = ZincCatalogPathHelper().path_for_manifest(manifest)
        filename = os.path.split(path)[-1]
        self.assertEquals(filename, 'zoo-1.json')

    def test_single_file_bundle_does_not_create_archive(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_bundle_version(catalog, "meep", self.scratch_dir)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertFalse(self.path_exists_in_catalog(archive_path))

    def test_more_than_one_file_bundle_does_create_archive(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        create_bundle_version(catalog, "meep", self.scratch_dir)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertTrue(self.path_exists_in_catalog(archive_path))

    def test_single_file_flavor_does_not_create_archive(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        flavor_spec = ZincFlavorSpec.from_dict({'dummy': ['+ *']})
        create_bundle_version(catalog, "meep", self.scratch_dir,
                              flavor_spec=flavor_spec)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1,
                flavor='dummy')
        self.assertFalse(self.path_exists_in_catalog(archive_path))

    def test_skip_master_archive_and_no_flavor_specified(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        create_bundle_version(catalog, "meep", self.scratch_dir,
                              skip_master_archive=True)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertTrue(self.path_exists_in_catalog(archive_path))

    def test_skip_master_archive_and_flavor_specified(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        flavor_spec = ZincFlavorSpec.from_dict({'dummy': ['+ *']})
        create_bundle_version(catalog, "meep", self.scratch_dir,
                              flavor_spec=flavor_spec, skip_master_archive=True)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertFalse(self.path_exists_in_catalog(archive_path))

    def test_update_distro_basic(self):
        # set up
        catalog = self._build_test_catalog()
        bundle_name, distro = "meep", "master"

        # create 'master' distro at v1
        catalog.update_distribution(distro, bundle_name, 1)

        # verify
        version = catalog.index.version_for_bundle(bundle_name, distro)
        self.assertEquals(version, 1)

    def test_save_prev_distro_if_no_previous(self):
        # set up
        catalog = self._build_test_catalog()
        bundle_name, distro = "meep", "master"

        # create 'master' distro at v1
        catalog.update_distribution(distro, bundle_name, 1)

        # verify
        prev_distro = helpers.distro_previous_name(distro)
        prev_version = catalog.index.version_for_bundle(bundle_name, prev_distro)
        self.assertTrue(prev_version is None)

    def test_save_prev_distro_if_prev_exists(self):
        # set up
        catalog = self._build_test_catalog()
        bundle_name, distro = "meep", "master"

        # create 'master' distro at v1
        catalog.update_distribution(distro, bundle_name, 1)

        # create a bundle version 2
        create_random_file(self.scratch_dir)
        create_bundle_version(catalog, bundle_name, self.scratch_dir)

        # update 'master' distro to v2
        catalog.update_distribution(distro, bundle_name, 2)

        # verify
        prev_distro = helpers.distro_previous_name(distro)
        prev_version = catalog.index.version_for_bundle(bundle_name, prev_distro)
        self.assertEquals(prev_version, 1)

    def add_dummy_flavorspec(self, catalog, flavorspec_name):
        flavorspec_string = json.dumps({'dummy': ['+ *']})
        subpath = catalog.path_helper.path_for_flavorspec_name(flavorspec_name)
        catalog._storage.puts(subpath, flavorspec_string)

    def test_update_flavorspec(self):
        #set up
        catalog = self._build_test_catalog()
        flavorspec_string = json.dumps({'dummy': ['+ *']})

        # add the flavorspec
        catalog.update_flavorspec_from_json_string("dummy", flavorspec_string)

        # verify
        expected_path = catalog._ph.path_for_flavorspec_name("dummy")
        self.path_exists_in_catalog(expected_path)

    def test_list_flavorspec(self):
        # set up
        catalog = self._build_test_catalog()
        self.add_dummy_flavorspec(catalog, "test")

        # get list
        actual_names = catalog.get_flavorspec_names()

        # verify
        self.assertEquals(["test"], actual_names)

    def test_delete_flavorspec(self):
        # set up
        catalog = self._build_test_catalog()
        self.add_dummy_flavorspec(catalog, "test")

        # delete flavorspec
        catalog.delete_flavorspec("test")

        # verify
        subpath = catalog.path_helper.path_for_flavorspec_name("test")
        self.assertFalse(os.path.exists(os.path.join(self.dir, subpath)))

