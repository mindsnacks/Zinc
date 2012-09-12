from tests import *
from zinc.tasks.bundle_create import ZincBundleCreateTask
from zinc.pathfilter import PathFilter
from zinc.models import ZincFlavorSpec

class TestZincBundleCloneTask(unittest.TestCase):
    pass

class TestZincBundleCreateTask(ZincCatalogBaseTestCase):

    def test_archive_is_not_created(self):
        """Tests that archives are not created if option is specified by task"""

        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')

        f1 = create_random_file(self.scratch_dir)
        f2 = create_random_file(self.scratch_dir)
        task = ZincBundleCreateTask(catalog, "beep", self.scratch_dir,
                create_archives=False)
        task.run()

        archive_path = catalog._path_for_archive_for_bundle_version(
                "beep", 1)
        self.assertFalse(os.path.exists(archive_path))

    def test_archive_is_created_master(self):

        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')

        f1 = create_random_file(self.scratch_dir)
        f2 = create_random_file(self.scratch_dir)
        task = ZincBundleCreateTask(catalog, "beep", self.scratch_dir,
                create_archives=True)
        task.run()

        archive_path = catalog._path_for_archive_for_bundle_version(
                "beep", 1)
        self.assertTrue(os.path.exists(archive_path))

    def test_archive_is_created_flavor(self):

        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')

        path_filter = PathFilter.from_rule_list(["+ *",]) # match everything
        flavor_spec = ZincFlavorSpec()
        flavor_spec.add_flavor("all", path_filter)

        f1 = create_random_file(self.scratch_dir)
        f2 = create_random_file(self.scratch_dir)
        task = ZincBundleCreateTask(catalog, "beep", self.scratch_dir,
                flavor_spec=flavor_spec, create_archives=True)
        task.run()

        archive_path = catalog._path_for_archive_for_bundle_version(
                "beep", 1, flavor="all")
        self.assertTrue(os.path.exists(archive_path))






