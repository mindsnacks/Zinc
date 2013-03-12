from zinc.client import *

from tests import *

class TestZincClientConfigFromFile(unittest.TestCase):

    def setUp(self):
        path = abs_path_for_fixture("zinc-client.config")
        self.config = ZincClientConfig.from_path(path)

    def test_load_from_file(self):
        self.assertTrue(self.config is not None)

    def test_load_bookmark(self):
        remote = self.config.bookmarks["remote"]
        self.assertEquals(remote['ref'], "http://foo.com/catalog/")

    def test_replace_vars(self):
        local = self.config.bookmarks["local"]
        print local
        print local['password']
        self.assertEquals(local['password'], "abc123")


class TestCatalogRefParsing(unittest.TestCase):

    def test_web(self):
        catalog_ref = 'http://localhost:5000/com.foo'
        r = catalog_ref_split(catalog_ref)
        self.assertEquals(r.service, 'http://localhost:5000/')
        self.assertEquals(r.catalog.id, 'com.foo')
        self.assertIsNone(r.catalog.loc)

    def test_path(self):
        catalog_ref = '/tmp/com.foo'
        r = catalog_ref_split(catalog_ref)
        self.assertEquals(r.service, catalog_ref)
        self.assertEquals(r.catalog.loc, '.')
        self.assertIsNone(r.catalog.id)

