import unittest

from zinc.pathfilter import PathFilter
from zinc.pathfilter import Match


class TestPathFilterRuleMatching(unittest.TestCase):

    def test_no_match_accept(self):
        r = PathFilter.Rule('a', Match.ACCEPT)
        self.assertEqual(r.match('b'), Match.UNKNOWN)

    def test_no_match_reject(self):
        r = PathFilter.Rule('a', Match.REJECT)
        self.assertEqual(r.match('b'), Match.UNKNOWN)

    def test_exact_accept(self):
        r = PathFilter.Rule('/path', Match.ACCEPT)
        self.assertEqual(r.match('/path'), Match.ACCEPT)

    def test_exact_reject(self):
        r = PathFilter.Rule('/path', Match.REJECT)
        self.assertEqual(r.match('/path'), Match.REJECT)

    def test_end_of_path_match(self):
        r = PathFilter.Rule('*/honk', Match.ACCEPT)
        self.assertEqual(r.match('beep/honk'), Match.ACCEPT)

    def test_middle_of_path_match(self):
        r = PathFilter.Rule('*/honk/*', Match.ACCEPT)
        self.assertEqual(r.match('/beep/honk/boop'), Match.ACCEPT)


class TestPathFilterMatching(unittest.TestCase):

    def test_empty(self):
        pf = PathFilter([])
        self.assertTrue(pf.match('/some/random/path'))

    def test_accept_all_in_dir(self):
        pf = PathFilter([
            PathFilter.Rule('*/100/*', Match.ACCEPT),
            PathFilter.Rule('*', Match.REJECT)])
        self.assertTrue(pf.match('/this/is/valid/100/file.png'))
        self.assertTrue(pf.match('/this/is/valid/100/file.jpg'))
        self.assertFalse(pf.match('/this/is/not/valid/20/file.png'))

    def test_accept_all_in_dir_with_exention(self):
        pf = PathFilter([
            PathFilter.Rule('*/100/*.png', Match.ACCEPT),
            PathFilter.Rule('*', Match.REJECT)])
        self.assertTrue(pf.match('/this/is/valid/100/file.png'))
        self.assertFalse(pf.match('/this/is/not/valid/20/file.png'))
        self.assertFalse(pf.match('/this/is/not/valid/100/file.jpg'))

    def test_reject_all_in_dir(self):
        pf = PathFilter([
            PathFilter.Rule('*/100/*', Match.REJECT)])
        self.assertFalse(pf.match('/this/is/valid/100/file.png'))
        self.assertFalse(pf.match('/this/is/valid/100/file.jpg'))
        self.assertTrue(pf.match('/this/is/not/valid/20/file.png'))

    def test_read_json(self):
        pf = PathFilter.from_rule_list(['+ a'])
        self.assertTrue(pf is not None)
        pf = PathFilter.from_rule_list(['- a'])
        self.assertTrue(pf is not None)

    def test_read_json_invalid(self):
        self.assertRaises(Exception, PathFilter.from_rule_list, ['? a'])
