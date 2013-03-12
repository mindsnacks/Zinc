from zinc.helpers import *

from tests import *

class TestZincHelpers(unittest.TestCase):

    def test_make_bundle_id(self):
        self.assertEquals(
                make_bundle_id('com.foo', 'bundle'),
                'com.foo.bundle')
        self.assertRaises(
                Exception,
                make_bundle_id, 'com.foo', None) 
        self.assertRaises(
                Exception,
                make_bundle_id, None, 'bundle') 

    def test_make_bundle_descriptor(self):
        self.assertEquals(
                make_bundle_descriptor(
                    'com.foo.bundle', 1),
                'com.foo.bundle-1')
        self.assertEquals(
                make_bundle_descriptor(
                    'com.foo.bundle', 1, 'small'),
                'com.foo.bundle-1~small')
        self.assertRaises(
                Exception,
                make_bundle_descriptor, None, 1) 
        self.assertRaises(
                Exception,
                make_bundle_descriptor, 'com.foo.bundle', None) 

