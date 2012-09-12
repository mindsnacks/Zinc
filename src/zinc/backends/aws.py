from . import IndexBackend, StorageBackend

import boto
from boto.s3.connection import S3Connection


class GenericAWSBackend(object):

    def __init__(self):
        pass


class SimpleDBIndexBackend(IndexBackend):

    def __init__(self,
            aws_key=None,
            aws_secret=None
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
            bucket=bucket):

        self.bucket = bucket
        
        self.s3 = S3Connection(aws_key, aws_secret)

    def write(self):
        pass


