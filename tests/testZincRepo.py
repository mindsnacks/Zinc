from tests import *
#from zinc import ZincRepo, create_repo_at_path, ZINC_FORMAT
from zinc import *
import os.path

class ZincRepoTestCase(TempDirTestCase):

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.repo_dir = os.path.join(self.dir, "repo")
        os.mkdir(self.repo_dir)
        self.scratch_dir = os.path.join(self.dir, "scratch")
        os.mkdir(self.scratch_dir)

    def test_repo_create(self):
        repo = create_repo_at_path(self.repo_dir)
        assert repo is not None
        assert repo.is_loaded() == True
        assert len(repo.verify()) == 0
        assert len(repo.bundle_names()) == 0
        assert repo.format() == ZINC_FORMAT

    def test_repo_create_bundle(self):
        repo = create_repo_at_path(self.repo_dir)
        bundle = repo.add_bundle("beep")
        assert bundle is not None
 
    def test_repo_create_duplicate_bundle(self):
        repo = create_repo_at_path(self.repo_dir)
        bundle1 = repo.add_bundle("beep")
        assert bundle1 is not None
        self.assertRaises(ValueError, repo.add_bundle, "beep")

    def test_repo_read_invalid_format(self):
        create_repo_at_path(self.repo_dir)
        index_path = os.path.join(self.repo_dir, "index.json")
        index = load_index(index_path)
        index.format = 2
        index.write(index_path)
        self.assertRaises(Exception, ZincRepo, (self.repo_dir))

    def test_repo_import_file(self):
        repo = create_repo_at_path(self.repo_dir)
        f1 = create_random_file(self.scratch_dir)
        repo.import_path(f1)

    def test_bundle_add_version_without_repo(self):
        bundle = ZincBundle("honk")
        assert bundle is not None
        self.assertRaises(Exception, bundle.add_version)



       
   

