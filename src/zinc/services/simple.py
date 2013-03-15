from multiprocessing import Process, Pipe

from zinc.coordinators.filesystem import FilesystemCatalogCoordinator
from zinc.storages.filesystem import FilesystemStorageBackend
from zinc.models import ZincIndex, ZincCatalogConfig
from . import ZincServiceConsumer, ZincServiceProvider, ZincCatalog

from zinc.defaults import defaults
from zinc.utils import *
from zinc.helpers import *


def f(service, conn, command):
    conn.send("fart")
    conn.close()


def _get_index(url, conn):
    storage = FilesystemStorageBackend(url=url)
    coordinator = FilesystemCatalogCoordinator(url=url)
    catalog = ZincCatalog(coordinator=coordinator, storage=storage)
    conn.send(catalog.get_index())
    conn.close()


class SimpleServiceConsumer(ZincServiceConsumer):

    def __init__(self, root_path=None):
        self._root_path = root_path or '/'

    def _abs_path(self, subpath):
        return os.path.join(canonical_path(self._root_path), subpath)

    def create_catalog(self, id=None, loc=None):
        assert id
        loc = loc or '.'

        path = self._abs_path(loc)
        makedirs(path)

        config_path = os.path.join(path, defaults['catalog_config_name'])
        ZincCatalogConfig().write(config_path)

        index_path = os.path.join(path, defaults['catalog_index_name'])
        ZincIndex(id).write(index_path)

    def get_catalog(self, loc=None, id=None):
        loc = loc or '.'
        url = file_url(os.path.join(self._root_path, loc))
        storage = FilesystemStorageBackend(url=url)
        coordinator = FilesystemCatalogCoordinator(url=url)
        return ZincCatalog(coordinator=coordinator, storage=storage)

    #def get_index(self):
    #    parent_conn, child_conn = Pipe()
    #    p = Process(target=_get_index, args=(self.url, child_conn))
    #    p.start()
    #    index = parent_conn.recv()
    #    p.join()
    #    return index


    ## TODO: tmp
    #def process_command(self, command):
    #    parent_conn, child_conn = Pipe()
    #    p = Process(target=f, args=(self, child_conn, command))
    #    p.start()
    #    print parent_conn.recv()
    #    p.join()


