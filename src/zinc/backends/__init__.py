

class IndexBackend(object):

    def lock_bundle(self, timeout=None):
        raise NotImplementedError
 
    def unlock_bundle(self):
        raise NotImplementedError


class StorageBackend(object):

    def write(self, data):
        pass
