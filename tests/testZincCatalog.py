import os.path
import logging

from zinc.models import ZincIndex, ZincManifest, ZincFlavorSpec
from zinc.catalog import (create_catalog_at_path, ZincCatalogPathHelper)
from zinc.defaults import defaults
from zinc.services import ZincCatalog
from zinc.storages import StorageBackend

from zinc.client import ZincClient, connect

from tests import *

class StorageBackendTestCase(unittest.TestCase):

    def setUp(self):
        self.storage = StorageBackend(url='file:///tmp')

    def test_get_raises(self):
        self.assertRaises(NotImplementedError, self.storage.get, 'foo')

    def test_get_meta_raises(self):
        self.assertRaises(NotImplementedError, self.storage.get_meta, 'foo')

    def test_put_raises(self):
        self.assertRaises(NotImplementedError, self.storage.put , 'foo', 'bar')


class ZincCatalogTestCase(TempDirTestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
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
        #self.assertTrue(catalog.is_loaded() == True) #TODO: replace?
        self.assertTrue(len(catalog.verify()) == 0)
        self.assertTrue(len(catalog.index.bundle_names()) == 0)
        self.assertTrue(catalog.format() == defaults['zinc_format'])

    def test_catalog_create_manifest(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        manifest = catalog._add_manifest("beep")
        self.assertTrue(manifest is not None)
 
    def test_catalog_create_duplicate_manifest(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        manifest1 = catalog._add_manifest("beep")
        self.assertTrue(manifest1 is not None)
        self.assertRaises(ValueError, catalog._add_manifest, "beep")

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

    def test_version_for_bundle(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        catalog._add_manifest("meep", 1)
        versions = catalog.index.versions_for_bundle("meep")
        self.assertTrue(1 in versions)
        self.assertTrue(len(versions) == 1)

    def test_versions_for_nonexistant_bundle(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        versions = catalog.index.versions_for_bundle("meep")
        self.assertTrue(len(versions) == 0)

    def _build_test_catalog(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        client = connect(self.catalog_dir)
        client.create_bundle_version("meep", self.scratch_dir)
        catalog._reload() # TODO: fix/remove/something
        return catalog

    def test_create_bundle_version(self):
        catalog = self._build_test_catalog()
        client = connect(self.catalog_dir)
        self.assertTrue("meep" in client.catalog_index().bundle_names())
        self.assertTrue(1 in client.catalog_index().versions_for_bundle("meep"))
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
        ZincClient(catalog). create_bundle_version("meep", self.scratch_dir)
        results = catalog.verify()
        
    def test_create_second_bundle_version(self):
        catalog = self._build_test_catalog()
        # add a file
        create_random_file(self.scratch_dir)
        client = connect(self.catalog_dir)
        client.create_bundle_version("meep", self.scratch_dir)
        self.assertTrue(2 in client.catalog_index().versions_for_bundle("meep"))
        new_index = ZincIndex.from_path(os.path.join(catalog.path, defaults['catalog_index_name']))
        self.assertTrue(1 in new_index.versions_for_bundle("meep"))
        self.assertTrue(2 in new_index.versions_for_bundle("meep"))

    def test_create_duplicate_bundle_version_no_force(self):
        catalog = self._build_test_catalog()
        # add a file
        create_random_file(self.scratch_dir)
        # create first version
        client = connect(self.catalog_dir)
        manifest1 = client.create_bundle_version("meep", self.scratch_dir)
        self.assertTrue(2 in client.catalog_index().versions_for_bundle("meep"))
        # attempt to create same version again
        manifest2 = client.create_bundle_version("meep", self.scratch_dir)
        self.assertEquals(manifest1.version, manifest2.version)

    def test_create_duplicate_bundle_version_with_force(self):
        catalog = self._build_test_catalog()
        # add a file
        create_random_file(self.scratch_dir)
        # create first version
        client = connect(self.catalog_dir)
        manifest1 = client.create_bundle_version("meep", self.scratch_dir)
        self.assertTrue(2 in client.catalog_index().versions_for_bundle("meep"))
        # attempt to create same version again, with force
        manifest2 = client.create_bundle_version(
                "meep", self.scratch_dir, force=True)
        self.assertNotEquals(manifest1.version, manifest2.version)


    def test_create_identical_bundle_version(self):
        catalog = self._build_test_catalog()
        client = connect(self.catalog_dir)
        client.create_bundle_version("meep", self.scratch_dir)
        self.assertEquals(len(client.catalog_index().versions_for_bundle("meep")), 1)

    def test_catalog_verify(self):
        catalog = self._build_test_catalog()
        results = catalog.verify()

    def test_path_for_manifest_with_name_version(self):
        catalog = self._build_test_catalog()
        manifest = ZincManifest(catalog.index.id, 'zoo', 1)
        path = ZincCatalogPathHelper().path_for_manifest(manifest)
        filename = os.path.split(path)[-1]
        self.assertEquals(filename, 'zoo-1.json')

    def test_single_file_bundle_does_not_create_archive(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        ZincClient(catalog).create_bundle_version("meep", self.scratch_dir)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertFalse(self.path_exists_in_catalog(archive_path))

    def test_more_than_one_file_bundle_does_create_archive(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        ZincClient(catalog).create_bundle_version("meep", self.scratch_dir)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertTrue(self.path_exists_in_catalog(archive_path))

    def test_single_file_flavor_does_not_create_archive(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        flavor_spec = ZincFlavorSpec.from_dict({'dummy': ['+ *']})
        ZincClient(catalog).create_bundle_version("meep", self.scratch_dir,
                flavor_spec=flavor_spec)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1,
                flavor='dummy')
        self.assertFalse(self.path_exists_in_catalog(archive_path))

    def test_skip_master_archive_and_no_flavor_specified(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        ZincClient(catalog).create_bundle_version(
                "meep", self.scratch_dir, skip_master_archive=True)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertTrue(self.path_exists_in_catalog(archive_path))

    def test_skip_master_archive_and_flavor_specified(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        create_random_file(self.scratch_dir)
        flavor_spec = ZincFlavorSpec.from_dict({'dummy': ['+ *']})
        ZincClient(catalog).create_bundle_version(
                "meep", self.scratch_dir, flavor_spec=flavor_spec, skip_master_archive=True)
        archive_path = ZincCatalogPathHelper().path_for_archive_for_bundle_version("meep", 1)
        self.assertFalse(self.path_exists_in_catalog(archive_path))

    def test_next_version_is_2_for_new_bundle(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        create_random_file(self.scratch_dir)
        client = connect(self.catalog_dir)
        client.create_bundle_version("meep", self.scratch_dir)
        next_version = client.catalog_index().next_version_for_bundle("meep")
        self.assertEquals(next_version, 2)

    ## covered by ZincIndexTestCase
    #def test_next_version_is_added_if_missing(self):
    #    catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')

    #    client = connect(self.catalog_dir)
    #   
    #    # create v1
    #    create_random_file(self.scratch_dir)
    #    client.create_bundle_version("meep", self.scratch_dir)

    #    client = None
    #    
    #    # remove the 'next_version' key
    #    del catalog.index._bundle_info_by_name["meep"]["next_version"]
    #   
    #    # create v2
    #    create_random_file(self.scratch_dir)
    #    client.create_bundle_version("meep", self.scratch_dir)

    #    # check
    #    next_version = catalog.index.next_version_for_bundle("meep")
    #    self.assertEquals(next_version, 3)


