# -*- coding: utf-8 -*-

"""
zinc.utils
~~~~~~~~~~

This module provides utility functions that are used within Zinc.

"""

import hashlib
import gzip
import zlib
import os

def sha1_for_path(path):
    """Returns the SHA1 hash as a string for the given path."""
    sha1 = hashlib.sha1()
    f = open(path, 'rb')
    try:
        sha1.update(f.read())
    finally:
        f.close()
    return sha1.hexdigest()

def canonical_path(path):
    path = os.path.expanduser(path)
    path = os.path.normpath(path)
    path = os.path.realpath(path)
    return path

def makedirs(path):
    """Convenience method that ignores errors if directory already exists."""
    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno == 17:
            pass # directory already exists
        else:
            raise e

def gzip_path(src_path, dst_path):
    """Convenience method for gzipping a file."""
    f_in = open(src_path, 'rb')
    f_out = gzip.open(dst_path, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

def gunzip_path(src_path, dst_path):
    """Convenience method for un-gzipping a file."""
    f_in = gzip.open(src_path, 'rb')
    f_out = open(dst_path, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

def gzip_bytes(bytes):
    """Convenience method for gzipping bytes in memory."""
    return zlib.compress(bytes)


