from . import IndexBackend, StorageBackend

import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os


class GenericAWSBackend(object):

    def __init__(self):
        pass


class SimpleDBIndexBackend(IndexBackend):

    def __init__(self,
            aws_key=None,
            aws_secret=None,
            domain=None):

        self.domain = domain

        self.db = boto.connect_sdb(aws_key, aws_secret)

    def lock(self):
        pass

    def unlock(self):
        pass


class S3StorageBackend(StorageBackend):

    def __init__(self,
            aws_key=None,
            aws_secret=None,
            bucket_name=None):

        self.bucket_name = bucket_name
        
        self.s3 = S3Connection(aws_key, aws_secret)
        self.bucket = self.s3.get_bucket(bucket_name)

        self.prefix = None


    def _prefixed_path(self, path):
        if self.prefix is None: return path
        return os.path.join(self.prefix, path)

    #def write_data(self, data, rel_path, raw=True, gzip=False):
    #    pass

    #def write_json_dict(self, json_dict, rel_path, raw=True, gzip=False):
    #    pass
    
    def write_path(self, src_path, rel_path):
        dst_path = self._prefixed_path(rel_path)
        k = Key(self.bucket)
        k.key = dst_path
        k.set_contents_from_filename(src_path)

    def size_for_path(self, rel_path):
        dst_path = self._prefixed_path(rel_path)
        k = Key(self.bucket)
        k.lookup(dst_path)
        return k.size




