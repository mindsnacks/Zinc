import os
from tempfile import TemporaryFile
from copy import copy
from urllib.parse import urlparse
import logging

import boto3

from . import StorageBackend

log = logging.getLogger(__name__)


class S3StorageBackend(StorageBackend):

    def __init__(self, url=None, aws_key=None, aws_secret=None,
                 s3connection=None, bucket=None, prefix=None, **kwargs):

        super(S3StorageBackend, self).__init__(**kwargs)

        assert s3connection or (aws_key and aws_secret)

        assert bucket or url
        bucket_name = bucket or urlparse(url).netloc
        session = boto3.session.Session(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        connection = session.resource('s3')
        self._bucket = self._get_bucket(bucket_name, connection)
        self._prefix = prefix

    def _get_bucket(self, bucket_name, connection):
        import botocore.exceptions
        bucket = connection.Bucket(bucket_name)
        exists = True
        try:
            connection.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as error:
            error_code = error.response['Error']['Code']
            if error_code == '404':
                exists = False
            else:
                raise error
        if exists:
            return bucket
        else:
            return None

    @classmethod
    def valid_url(cls, url):
        return urlparse(url).scheme in ('s3')

    def bind_to_catalog(self, id=None):
        assert id
        cpy = copy(self)
        cpy._prefix = id
        return cpy

    def _get_keyname(self, subpath):
        if self._prefix:
            return os.path.join(self._prefix, subpath)
        else:
            return subpath

    def get(self, subpath):
        keyname = self._get_keyname(subpath)
        t = TemporaryFile(mode="w+b")
        self._bucket.download_fileobj(keyname, t)
        if t.tell() == 0:
            return None
        t.seek(0)
        return t

    def get_meta(self, subpath):

        keyname = self._get_keyname(subpath)
        object_summary_iterator = self._bucket.objects.filter(Prefix=keyname, MaxKeys=1)
        object_summaries = list(object_summary_iterator)
        if len(object_summaries) == 0:
            return None
        object_summary = object_summaries[0]
        meta = dict()
        meta['size'] = object_summary.size
        return meta

    def put(self, subpath, fileobj, max_age=None, **kwargs):
        keyname = self._get_keyname(subpath)
        self._bucket.upload_fileobj(fileobj, keyname)

    def list(self, prefix=None):
        contents = []
        subpath = self._get_keyname(prefix)
        for object_summary in self._bucket.objects.filter(prefix=subpath):
            if object_summary.key.endswith("/"):
                # skip "directory" keys
                continue
            rel_path = object_summary.key[len(subpath) + 1:]
            contents.append(rel_path)
        return contents

    def delete(self, subpath):
        self._bucket.objects.delete([self._get_keyname(subpath)])
