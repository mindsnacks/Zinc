
#from flask import Flask, request, redirect, abort
import requests
from urlparse import urljoin
import os

from zinc.catalog import ZincAbstractCatalog
from zinc.models import ZincIndex
from . import ZincServiceConsumer


class WebServiceZincCatalog(ZincAbstractCatalog):

    def __init__(self, id, service_consumer):
        self._service_consumer = service_consumer
        self._id = id

    @property
    def id(self):
        return self._id

    def _url_for_path(self, path):
        return urljoin(self._service_consumer.url, path)

    def _get_url(self, url):
        return requests.get(url)

    def get_index(self):
        url = self._url_for_path(
                os.path.join(self.id, 'index.json')) # TODO: improve this
        r = self._get_url(url)
        return ZincIndex.from_bytes(r.content, mutable=False)


class WebServiceConsumer(ZincServiceConsumer):

    def __init__(self, url=None):
        assert url
        self._url = url

    @property
    def url(self):
        return self._url

    def get_catalog(self, loc=None, id=None):
        assert id
        return WebServiceZincCatalog(id, self)
