import os
from multiprocessing import Process, Pipe
from urlparse import urlparse

from zinc.coordinators.filesystem import FilesystemCatalogCoordinator
from zinc.storages.filesystem import FilesystemStorageBackend
from zinc.models import ZincIndex, ZincCatalogConfig
from . import ZincServiceConsumer, ZincServiceProvider, ZincCatalog

from zinc.defaults import defaults
import zinc.utils as utils


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

    def __init__(self, url=None):
        assert url
        self._url = url

    def _root_abs_path(self):
        return urlparse(self._url).path

    def _abs_path(self, subpath):
        return os.path.join(self._root_abs_path(), subpath)

    ## TODO: fix cloning between this and zinc.client
    def create_catalog(self, id=None, loc=None):
        assert id
        loc = loc or '.'

        path = self._abs_path(loc)
        utils.makedirs(path)

        config_path = os.path.join(path, defaults['catalog_config_name'])
        ZincCatalogConfig().write(config_path)

        index_path = os.path.join(path, defaults['catalog_index_name'])
        ZincIndex(id).write(index_path)

    def get_catalog(self, loc=None, id=None, **kwargs):
        loc = loc or '.'
        url = utils.file_url(os.path.join(self._root_abs_path(), loc))
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
