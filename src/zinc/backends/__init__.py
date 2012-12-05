from zinc.defaults import defaults

class IndexBackend(object):

    def __init__(self):
        self.lock_timeout = defaults['lock_timeout']

    def lock_bundle(self, bundle_name, timeout=None):
        raise NotImplementedError
 
    def unlock_bundle(self, bundle_name):
        raise NotImplementedError

    def _add_version_for_bundle(self, bundle_name):
        pass

    #def add_version_for_bundle(self, bundle_name):
    #    lock = self.lock_bundle(bundle_name, timeout=self.lock_timeout)
    #    if lock is None:
    #        raise Exception("Couldn't lock")


class StorageBackend(object):

    def write(self, data):
        pass
