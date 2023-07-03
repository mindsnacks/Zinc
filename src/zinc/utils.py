# -*- coding: utf-8 -*-

"""
zinc.utils
~~~~~~~~~~

This module provides utility functions that are used within Zinc.

"""

from __future__ import absolute_import

import hashlib
import gzip
import zlib
import os
from io import BytesIO

from types import GeneratorType
from itertools import tee

Tee: type = tee([], 1)[0].__class__


class EnumMC(type):
    def __contains__(self, val):
        return val in vars(self).values()


def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return EnumMC('Enum', (), enums)


def memoized(f):
    cache = dict()

    def ret(*args):
        if args not in cache:
            cache[args] = f(*args)
        if isinstance(cache[args], (GeneratorType, Tee)):
            # the original can't be used any more,
            # so we need to change the cache as well
            cache[args], r = tee(cache[args])
            return r
        return cache[args]
    return ret


def sha1_for_path(path: str) -> str:
    """Returns the SHA1 hash as a string for the given path."""
    sha1 = hashlib.sha1()
    f = open(path, 'rb')
    try:
        sha1.update(f.read())
    finally:
        f.close()
    return sha1.hexdigest()


def canonical_path(path: str) -> str:
    path = os.path.expanduser(path)
    path = os.path.normpath(path)
    path = os.path.realpath(path)
    return path


def makedirs(path: str):
    """Convenience method that ignores errors if directory already exists."""
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == 17:
            pass  # directory already exists
        else:
            raise e


def gzip_path(src_path: str, dst_path: str) -> None:
    """Convenience method for gzipping a file."""
    f_in = open(src_path, 'rb')
    f_out = gzip.open(dst_path, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()


def gunzip_path(src_path: str, dst_path: str) -> None:
    """Convenience method for un-gzipping a file."""
    f_in = gzip.open(src_path, 'rb')
    f_out = open(dst_path, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()


def gzip_bytes(bytes: bytes) -> bytes:
    """Convenience method for gzipping bytes in memory."""
    buffer = BytesIO()
    gzfile = gzip.GzipFile(fileobj=buffer, mode='wb')
    gzfile.write(bytes)
    gzfile.close()
    gz_bytes = buffer.getvalue()
    buffer.close()
    return gz_bytes


def gunzip_bytes(bytes: bytes) -> bytes:
    return zlib.decompress(bytes, 16 + zlib.MAX_WBITS)


def file_url(path: str) -> str:
    return 'file://%s' % (canonical_path(path))
