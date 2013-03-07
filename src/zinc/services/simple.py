from multiprocessing import Process, Pipe

from zinc.coordinators.filesystem import FilesystemCatalogCoordinator
from zinc.storages.filesystem import FilesystemStorageBackend
from . import ZincService, ZincCatalog

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


class SimpleService(ZincService):

    def __init__(self, path=None):
        assert path

        self.url = 'file://%s' % (canonical_path(path))
        #self._storage = FilesystemStorageBackend(url=url)
        #self._coordinator = FilesystemCatalogCoordinator(url=url, storage=self._storage)
        #self._catalog = ZincCatalog(coordinator=self._coordinator)


    def get_index(self):
        parent_conn, child_conn = Pipe()
        p = Process(target=_get_index, args=(self.url, child_conn))
        p.start()
        index = parent_conn.recv()
        p.join()
        return index


    ## TODO: tmp
    #def process_command(self, command):
    #    parent_conn, child_conn = Pipe()
    #    p = Process(target=f, args=(self, child_conn, command))
    #    p.start()
    #    print parent_conn.recv()
    #    p.join()


