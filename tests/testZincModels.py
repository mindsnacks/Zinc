from tests import *
from zinc import *
import os.path

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

    def test_distributions_for_bundle_by_version_unknown_bundle(self):
        """Tests that an exception is raised if called with an unknown bundle name"""
        index = ZincIndex()
        self.assertRaises(Exception, index.distributions_for_bundle_by_version, "meep")

    def test_distributions_for_bundle_by_version_no_distros(self):
        """Tests that the result is empty if no distros exist"""
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        distros = index.distributions_for_bundle_by_version("meep")
        self.assertEquals(len(distros), 0)

    def test_distributions_for_bundle_by_version_single_distro(self):
        """Tests that the result is correct if there is one distro associated
        with the version."""
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.update_distribution("master", "meep", 1)
        distros = index.distributions_for_bundle_by_version("meep")
        self.assertEquals(distros[1], ["master",])

    def test_distributions_for_bundle_by_version_multiple_distros(self):
        """Tests that the result is correct if there is one distro associated
        with the version."""
        index = ZincIndex()
        index.add_version_for_bundle("meep", 1)
        index.update_distribution("master", "meep", 1)
        index.update_distribution("test", "meep", 1)
        distros = index.distributions_for_bundle_by_version("meep")
        self.assertTrue("master" in distros[1])
        self.assertTrue("test" in distros[1])


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
        manifest2 = ZincManifest.from_path(path)
        assert manifest1.equals(manifest2)

    def test_save_and_load_with_flavors(self):
        manifest1 = ZincManifest('com.mindsnacks.test', 'meep', 1)
        manifest1._flavors = ['green']
        path = os.path.join(self.dir, "manifest.json")
        manifest1.write(path)
        manifest2 = ZincManifest.from_path(path)
        assert manifest1.equals(manifest2)

    def test_add_flavor(self):
        manifest = ZincManifest('com.mindsnacks.test', 'meep', 1)
        manifest.add_file('foo', 'ea502a7bbd407872e50b9328956277d0228272d4')
        manifest.add_flavor_for_file('foo', 'green')
        flavors = manifest.flavors_for_file('foo')
        self.assertEquals(len(flavors), 1)
        self.assertTrue('green' in flavors)
        self.assertTrue('green' in manifest.flavors)

class ZincFlavorSpecTestCase(unittest.TestCase):

    def test_load_from_dict_1(self):
        d = {'small' : ['+ 50x50'], 'large' : ['+ 100x100']}
        spec = ZincFlavorSpec.from_dict(d)
        self.assertTrue(spec is not None)
        self.assertEquals(len(spec.flavors), 2)

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

    def test_bundle_version_from_descriptor_with_flavor_with_dash(self):
        descriptor = 'com.foo.bar-1~green-ish'
        bundle_version = 1
        self.assertEquals(bundle_version, bundle_version_from_bundle_descriptor(descriptor))




