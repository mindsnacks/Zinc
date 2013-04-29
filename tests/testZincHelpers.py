import zinc.helpers as helpers

from tests import *


class TestZincHelpers(unittest.TestCase):

    def test_make_bundle_id(self):
        self.assertEquals(helpers.make_bundle_id('com.foo', 'bundle'),
                          'com.foo.bundle')
        self.assertRaises(Exception, helpers.make_bundle_id, 'com.foo', None)
        self.assertRaises(Exception, helpers.make_bundle_id, None, 'bundle')

    def test_make_bundle_descriptor(self):
        self.assertEquals(helpers.make_bundle_descriptor('com.foo.bundle', 1),
                          'com.foo.bundle-1')
        self.assertEquals(helpers.make_bundle_descriptor('com.foo.bundle', 1,
                                                         'small'),
                          'com.foo.bundle-1~small')
        self.assertRaises(Exception, helpers.make_bundle_descriptor, None, 1)
        self.assertRaises(Exception, helpers.make_bundle_descriptor,
                          'com.foo.bundle', None)

    def test_distro_name_errors_ok(self):
        errors = helpers.distro_name_errors("meep")
        self.assertTrue(len(errors) == 0)

    def test_distro_name_errors_bad_because_prev_prefix(self):
        errors = helpers.distro_name_errors("_meep")
        self.assertTrue(len(errors) == 1)

    def test_distro_name_errors_bad_because_zero_length(self):
        errors = helpers.distro_name_errors("")
        self.assertTrue(len(errors) == 1)
