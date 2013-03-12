from StringIO import StringIO


class StorageBackend(object):

    def puts(self, subpath, bytes):
        """Write string 'bytes' to subpath."""
        fileobj = StringIO(bytes)
        self.put(subpath, fileobj)

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

    def put(self, subpath, fileobj):
        """Write data from file-like object 'fileobj' to subpath."""
        raise NotImplementedError()


