import unittest
import tempfile
import random
import os
import sys

# make sure we can `import zinc.___`
testdir = os.path.dirname(__file__)
srcdir = '../src/'
sys.path.insert(0, os.path.abspath(os.path.join(testdir, srcdir)))


class TempDirTestCase(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        # print(self.dir)

    def tearDown(self):
        # shutil.rmtree(self.dir)
        pass


def create_random_file(dir, size=1024):
    path = tempfile.mkstemp(dir=dir)[1]
    file = open(path, 'w')
    for i in range(size):
        r = random.randint(0, 7)
        file.write(str(r))
    file.close()
    return path


def abs_path_for_fixture(relpath):
    mypath = os.path.abspath(__file__)
    mydir = os.path.dirname(mypath)
    return os.path.join(mydir, 'fixtures', relpath)
