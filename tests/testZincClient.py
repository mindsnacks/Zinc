from tests import *
from zinc import *
from zinc.client import ZincClientConfig

class TestZincClientConfig(unittest.TestCase):

    def setUp(self):
        path = abs_path_for_fixture("zinc-client.config")
        self.config = ZincClientConfig.from_path(path)

    def test_load_from_file(self):
        self.assertTrue(self.config is not None)

    def test_load_bookmark(self):
        remote_loc = self.config.bookmarks["remote"]
        self.assertEquals(remote_loc, "http://foo.com/catalog/")



