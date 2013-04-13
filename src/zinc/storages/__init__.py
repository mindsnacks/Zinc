from StringIO import StringIO


class StorageBackend(object):

    def __init__(self, url=None, **kwargs):

        if url is not None:
            assert self.valid_url(url)
            self._url = url
        else:
            self._url = None

    @classmethod
    def valid_url(cls, url):
        raise NotImplementedError()

    @property
    def url(self):
        return self._url

    # TODO: think of a better name
    def bind_to_catalog(self, id=None):
        raise NotImplementedError()

    def puts(self, subpath, bytes, **kwargs):
        """Write string 'bytes' to subpath."""
        fileobj = StringIO(bytes)
        self.put(subpath, fileobj, **kwargs)

    ## Methods to override

    def get(self, subpath):
        """Return file-like object at subpath."""
        raise NotImplementedError()

    def get_meta(self, subpath):
        """
        Return dictionary of metadata for item at subpath or None if subpath
        does not exist.

        Keys:
           - size: the size of the file
        """
        raise NotImplementedError()

    def put(self, subpath, fileobj, **kwargs):
        """Write data from file-like object 'fileobj' to subpath."""
        raise NotImplementedError()

    def list(self, prefix=None):
        """List contents, with optional prefix."""
        raise NotImplementedError()

    def delete(self, subpath):
        """Delete subpath."""
        raise NotImplementedError()


def storage_for_url(url):
    from .filesystem import FilesystemStorageBackend
    from .aws import S3StorageBackend

    storage_classes = (FilesystemStorageBackend, S3StorageBackend)

    for storage_class in storage_classes:
        if storage_class.valid_url(url):
            return storage_class
