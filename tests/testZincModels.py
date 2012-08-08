from tests import *
from zinc import *
from zinc.models import bundle_id_from_bundle_descriptor, bundle_version_from_bundle_descriptor
import os.path

class ZincCatalogTestCase(TempDirTestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.catalog_dir = os.path.join(self.dir, "catalog")
        os.mkdir(self.catalog_dir)
        self.scratch_dir = os.path.join(self.dir, "scratch")
        os.mkdir(self.scratch_dir)

    def test_catalog_create(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        assert catalog is not None
        assert catalog.is_loaded() == True
        assert len(catalog.verify()) == 0
        assert len(catalog.bundle_names()) == 0
        assert catalog.format() == defaults['zinc_format']

    def test_catalog_create_manifest(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        manifest = catalog._add_manifest("beep")
        assert manifest is not None
 
    def test_catalog_create_duplicate_manifest(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        manifest1 = catalog._add_manifest("beep")
        assert manifest1 is not None
        self.assertRaises(ValueError, catalog._add_manifest, "beep")

    def test_catalog_read_invalid_format(self):
        create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        index_path = os.path.join(self.catalog_dir, defaults['catalog_index_name'])
        index = load_index(index_path)
        index.format = 2
        index.write(index_path)
        self.assertRaises(Exception, ZincCatalog, (self.catalog_dir))

    def test_catalog_import_file(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        f1 = create_random_file(self.scratch_dir)
        catalog._import_path(f1)

    # TODO: kill test?
    #def test_bundle_add_version_without_catalog(self):
    #    bundle = ZincBundle("honk")
    #    assert bundle is not None
    #    self.assertRaises(Exception, bundle.add_version)

    def test_bundle_names_with_no_bundles(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        assert len(catalog.bundle_names()) == 0

    def test_version_for_bundle(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        catalog._add_manifest("meep", 1)
        versions = catalog.versions_for_bundle("meep")
        assert 1 in versions
        assert len(versions) == 1

    def test_versions_for_nonexistant_bundle(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        versions = catalog.versions_for_bundle("meep")
        assert len(versions) == 0

    def _build_test_catalog(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        f1 = create_random_file(self.scratch_dir)
        f2 = create_random_file(self.scratch_dir)
        catalog.create_bundle_version("meep", self.scratch_dir)
        return catalog

    def test_create_bundle_version(self):
        catalog = self._build_test_catalog()
        assert "meep" in catalog.bundle_names()
        assert 1 in catalog.versions_for_bundle("meep")
        manifest = catalog.manifest_for_bundle("meep", 1)
        assert manifest is not None
        for (file, props) in manifest.files.items():
            sha = props['sha']
            formats = props['formats']
            for format in formats.keys():
                ext = None
                if format == 'gz':
                    ext = '.gz'
                object_path = catalog._path_for_file_with_sha(file, sha, ext)
                assert os.path.exists(object_path)

    def test_bundle_name_in_manifest(self):
        catalog = self._build_test_catalog()
        bundle_name = "meep"
        manifest = catalog.manifest_for_bundle("meep", 1)
        assert manifest.bundle_name == bundle_name

    def test_create_bundle_with_subdirs(self):
        f1 = create_random_file(self.scratch_dir)
        one_dir = os.mkdir(os.path.join(self.scratch_dir, "one"))
        f2 = create_random_file(one_dir)
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        catalog.create_bundle_version("meep", self.scratch_dir)
        results = catalog.verify()
        
    def test_create_second_bundle_version(self):
        catalog = self._build_test_catalog()
        # add a file
        f3 = create_random_file(self.scratch_dir)
        catalog.create_bundle_version("meep", self.scratch_dir)
        self.assertTrue(2 in catalog.versions_for_bundle("meep"))
        new_index = load_index(os.path.join(catalog.path, defaults['catalog_index_name']))
        assert 1 in new_index.versions_for_bundle("meep")
        assert 2 in new_index.versions_for_bundle("meep")

    def test_create_identical_bundle_version(self):
        catalog = self._build_test_catalog()
        catalog.create_bundle_version("meep", self.scratch_dir)
        self.assertEquals(len(catalog.versions_for_bundle("meep")), 1)

    def test_catalog_verify(self):
        catalog = self._build_test_catalog()
        results = catalog.verify()

    def test_path_for_manifest_with_name_version(self):
        catalog = self._build_test_catalog()
        manifest = ZincManifest(catalog.index.id, 'zoo', 1)
        path = catalog._path_for_manifest(manifest)
        filename = os.path.split(path)[-1]
        self.assertEquals(filename, 'zoo-1.json')

    #def test_path_for_manifest_with_name_version_flavor(self):
    #    catalog = self._build_test_catalog()
    #    manifest = ZincManifest(catalog.index.id, 'zoo', 1, 'vanilla')
    #    path = catalog._path_for_manifest(manifest)
    #    filename = os.path.split(path)[-1]
    #    self.assertEquals(filename, 'zoo-1~vanilla.json')

class ZincIndexTestCase(TempDirTestCase):

    def test_versions_for_nonexistant_bundle(self):
        index = ZincIndex()
        assert len(index.versions_for_bundle("meep")) == 0

    def test_add_version_for_bundle(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        assert 1 in index.versions_for_bundle("meep")

    def test_add_duplicate_version_for_bundle(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.add_version_for_bundle("meep", 1)
        assert 1 in index.versions_for_bundle("meep")
        assert len(index.versions_for_bundle("meep")) == 1

    def test_del_version_for_nonexistant_bundle(self):
        index = ZincIndex()
        self.assertRaises(Exception, index.delete_bundle_version, "meep", 1)
        assert len(index.versions_for_bundle("meep")) == 0

    def test_del_version_for_bundle(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.add_version_for_bundle("meep", 2)
        index.delete_bundle_version("meep", 1)
        assert len(index.versions_for_bundle("meep")) == 1
        assert 2 in index.versions_for_bundle("meep")

    def test_del_nonexistant_version_for_bundle(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.delete_bundle_version("meep", 2)
        assert len(index.versions_for_bundle("meep")) == 1
        assert 1 in index.versions_for_bundle("meep")

    def test_del_nonexistant_version_for_bundle(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.delete_bundle_version("meep", 2)

    def test_del_version_for_bundle_in_active_distro_raises(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.update_distribution("live", "meep", 1)
        self.assertRaises(Exception, index.delete_bundle_version, "meep", 1)

    def test_update_distro_bad_bundle(self):
        index = ZincIndex()
        self.assertRaises(ValueError, index.update_distribution, "live", "beep", 1)

    def test_update_distro_bad_bundle_version(self):
        index = ZincIndex()
        index.add_version_for_bundle("beep", 1)
        self.assertRaises(ValueError, index.update_distribution, "live", "beep", 2)

    def test_update_distro_ok(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.update_distribution("live", "meep", 1)

    def test_delete_distro_ok(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.update_distribution("live", "meep", 1)
        assert len(index.distributions_for_bundle("meep")) == 1
        index.delete_distribution("live", "meep")
        assert len(index.distributions_for_bundle("meep")) == 0

    def test_version_for_distro(self):
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.update_distribution("live", "meep", 1)
        assert index.version_for_bundle("meep", "live") == 1

class ZincManifestTestCase(TempDirTestCase):

    def test_save_and_load_with_files(self):
        manifest1 = ZincManifest('com.mindsnacks.test', 'meep', 1)
        manifest1.files = {
                'a': {
                    'sha': 'ea502a7bbd407872e50b9328956277d0228272d4',
                    'formats': { 
                        'raw' : {
                            'size': 123
                            }
                        }
                    }
                }
        path = os.path.join(self.dir, "manifest.json")
        manifest1.write(path)
        manifest2 = load_manifest(path)
        assert manifest1.equals(manifest2)

    def test_save_and_load_with_flavors(self):
        manifest1 = ZincManifest('com.mindsnacks.test', 'meep', 1)
        manifest1._flavors = ['green']
        path = os.path.join(self.dir, "manifest.json")
        manifest1.write(path)
        manifest2 = load_manifest(path)
        assert manifest1.equals(manifest2)

    def test_add_flavor(self):
        manifest = ZincManifest('com.mindsnacks.test', 'meep', 1)
        manifest.add_file('foo', 'ea502a7bbd407872e50b9328956277d0228272d4')
        manifest.add_flavor_for_file('foo', 'green')
        flavors = manifest.flavors_for_file('foo')
        self.assertEquals(len(flavors), 1)
        self.assertTrue('green' in flavors)
        self.assertTrue('green' in manifest.flavors())

class ZincFlavorSpecTestCase(unittest.TestCase):

    def test_load_from_dict_1(self):
        d = {'small' : ['+ 50x50'], 'large' : ['+ 100x100']}
        spec = ZincFlavorSpec.from_dict(d)
        self.assertTrue(spec is not None)
        self.assertEquals(len(spec.flavors()), 2)

class BundleDescriptorTestCase(unittest.TestCase):

    def test_bundle_id_from_descriptor_without_flavor(self):
        descriptor = 'com.foo.bar-1'
        bundle_id = 'com.foo.bar'
        self.assertEquals(bundle_id, bundle_id_from_bundle_descriptor(descriptor))
        
    def test_bundle_id_from_descriptor_with_flavor(self):
        descriptor = 'com.foo.bar-1~green'
        bundle_id = 'com.foo.bar'
        self.assertEquals(bundle_id, bundle_id_from_bundle_descriptor(descriptor))

    def test_bundle_version_from_descriptor_without_flavor(self):
        descriptor = 'com.foo.bar-1'
        bundle_version = 1
        self.assertEquals(bundle_version, bundle_version_from_bundle_descriptor(descriptor))

    def test_bundle_version_from_descriptor_with_flavor(self):
        descriptor = 'com.foo.bar-1~green'
        bundle_version = 1
        self.assertEquals(bundle_version, bundle_version_from_bundle_descriptor(descriptor))



