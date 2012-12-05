import unittest
import tempfile
import shutil
import random
import base64
import os

from zinc.backends.filesystem import create_catalog_at_path

def create_random_file(dir, size=1024):
    path = tempfile.mkstemp(dir=dir)[1]
    file = open(path, 'w')
    for i in range(size):
        r = random.randint(0,7)
        file.write(str(r))
    file.close()
    return path


class TempDirTestCase(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        print self.dir
    
    def tearDown(self):
        #shutil.rmtree(self.dir)
        pass


class ZincCatalogBaseTestCase(TempDirTestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.catalog_dir = os.path.join(self.dir, "catalog")
        os.mkdir(self.catalog_dir)
        self.scratch_dir = os.path.join(self.dir, "scratch")
        os.mkdir(self.scratch_dir)

    def _build_test_catalog(self):
        catalog = create_catalog_at_path(self.catalog_dir, 'com.mindsnacks.test')
        f1 = create_random_file(self.scratch_dir)
        f2 = create_random_file(self.scratch_dir)
        catalog.create_bundle_version("meep", self.scratch_dir)
        return catalog



