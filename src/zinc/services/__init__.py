import logging

from zinc.catalog import ZincCatalog

log = logging.getLogger(__name__)

################################################################################


class ZincServiceProvider(object):
    pass


class ZincServiceConsumer(object):

    def __init__(self, **kwargs):
        pass

    def create_catalog(self, id=None, loc=None):
        raise NotImplementedError()

    def get_catalog(self, loc=None, id=None, lock_timeout=None):
        raise NotImplementedError()


class CustomServiceConsumer(ZincServiceConsumer):

    def __init__(self, coordinator=None, storage=None, **kwargs):
        assert coordinator
        assert storage

        self._coordinator = coordinator
        self._storage = storage

    def get_catalog(self, loc=None, id=None, lock_timeout=None):
        cat_storage = self._storage.bind_to_catalog(id=id)
        # TODO: bind to coordinator?
        return ZincCatalog(coordinator=self._coordinator, storage=cat_storage,
                           lock_timeout=lock_timeout)
