
################################################################################

class CatalogCoordinator(object):

    def __init__(self, url=None):
        if url is not None:
            assert self.validate_url(url)
            self._url = url
        else:
            self._url = None

    @property
    def url(self):
        return self._url

    def validate_url(self, url):
        raise NotImplementedError()

    def get_index_lock(self, prefix=None):
        raise NotImplementedError()

