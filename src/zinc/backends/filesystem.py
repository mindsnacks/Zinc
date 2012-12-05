import os
import json
from shutil import copyfile

from zinc import *
from zinc.backends import IndexBackend, StorageBackend
from zinc.models import ZincCatalog, ZincIndex
from zinc.utils import makedirs, canonical_path, gzip_path


def write_data(data, path, raw=True, gzip=False):
    if not raw and not gzip:
        raise Exception("Can't write nothing!")

    makedirs(os.path.dirname(path))
    with open(path, 'w') as file:
        file.write(data)
    if gzip:
        gzpath = path + '.gz'
        gzip_path(path, gzpath)

def write_json_dict(json_dict, path, raw=True, gzip=False):
    data = json.dumps(json_dict)
    write_data(data, path, gzip=gzip)

def write_path(src_path, dst_path):
    makedirs(os.path.dirname(dst_path))
    copyfile(src_path, dst_path)

def read_json_dict(path):
    with open(path, 'r') as file:
        json_dict = json.load(file)
    return json_dict


class FileSystemIndexBackend(IndexBackend):

    INDEX_FILENAME = 'index.json'
    CONFIG_FILENAME = 'config.json'
    
    def __init__(self, root_path):

        root_path = canonical_path(root_path)
        self.index_path = os.path.join(root_path, FileSystemIndexBackend.INDEX_FILENAME)
        self.config_path = os.path.join(root_path, FileSystemIndexBackend.CONFIG_FILENAME)

        self.lock_timeout = defaults['lock_timeout']

    def save_index(self, index):
        json_dict = index.to_dict()
        write_json_dict(json_dict, self.index_path)

    def load_index(self):
        with open(self.index_path, 'r') as index_file:
            json_dict = json.load(index_file)
        index = ZincIndex.from_dict(json_dict)
        index.backend = self
        if index.format != defaults['zinc_format']:
            raise Exception("Incompatible format %s" % (self.index.format))
        return index

    def lock_bundle(self, bundle_name, timeout=None):
        return True
 
    def unlock_bundle(self, bundle_name):
        pass

    def add_version_for_bundle(self, bundle_name):
        lock = self.lock_bundle(bundle_name, timeout=self.lock_timeout)
        if lock is None:
            raise Exception("Couldn't lock")



class FileSystemStorageBackend(StorageBackend):

    def __init__(self, root_path):
        self._path = root_path

    @property
    def path(self):
        return self._path

    def write_data(self, data, rel_path, raw=True, gzip=False):
        dst_path = os.path.join(self.path, rel_path)
        write_data(data, dst_path, raw=True, gzip=gzip)

    def write_json_dict(self, json_dict, rel_path, raw=True, gzip=False):
        dst_path = os.path.join(self.path, rel_path)
        write_json_dict(json_dict, dst_path, raw=True, gzip=gzip)

    def write_path(self, src_path, rel_path):
        dst_path = os.path.join(self.path, rel_path)
        write_path(src_path, dst_path)

    def read_json_dict(self, rel_path):
        src_path = os.path.join(self.path, rel_path)
        return read_json_dict(src_path)
       

def load_catalog_at_path(path):

    index_backend = FileSystemIndexBackend(path)
    storage_backend = FileSystemStorageBackend(path)
    return ZincCatalog(index_backend, storage_backend)

def create_catalog_at_path(path, id):

    makedirs(path)

    index_backend = FileSystemIndexBackend(path)
    index = ZincIndex(id=id, backend=index_backend)
    index.save() # TODO: shouldn't have to call this
    
    return load_catalog_at_path(path)

