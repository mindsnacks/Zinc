import unittest
import tempfile
import shutil
import random
import base64


class TempDirTestCase(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        #print self.dir
    
    def tearDown(self):
        shutil.rmtree(self.dir)

def create_random_file(dir, size=1024):
    path = tempfile.mkstemp(dir=dir)[1]
    file = open(path, 'w')
    for i in range(size):
        r = random.randint(0,7)
        file.write(str(r))
    file.close()
    return path

