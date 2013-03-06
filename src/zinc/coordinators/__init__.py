
################################################################################

class CatalogCoordinator(object):

    def __init__(self, url=None):
        assert url is not None
        assert self.validate_url(url)
        self._url = url

    @property
    def url(self):
        return self._url

    def validate_url(self, url):
        raise NotImplementedError()

    def get_index_lock(self):
        raise NotImplementedError()


