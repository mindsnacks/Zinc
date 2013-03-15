

class CatalogCoordinator(object):

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

    def get_index_lock(self, prefix=None):
        raise NotImplementedError()


################################################################################


def coordinator_for_url(url):
    # TODO: better way to search for coordinators
    from .filesystem import FilesystemCatalogCoordinator
    from .redis import RedisCatalogCoordinator

    coord_classes = (FilesystemCatalogCoordinator, RedisCatalogCoordinator)

    for coord_class in coord_classes:
        if coord_class.valid_url(url):
            return coord_class
