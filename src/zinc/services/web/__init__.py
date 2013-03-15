
#from flask import Flask, request, redirect, abort
import requests
from urlparse import urljoin

from zinc.catalog import ZincAbstractCatalog
from zinc.models import ZincIndex, ZincManifest
from zinc.services import ZincServiceConsumer


class WebServiceZincCatalog(ZincAbstractCatalog):

    def __init__(self, id, service_consumer):
        self._service_consumer = service_consumer
        self._id = id

    @property
    def id(self):
        return self._id

    def _url_for_path(self, path):
        return urljoin(self._service_consumer.url, path)

    def get_index(self):
        path = self.id  # + '/index.json' # TODO: improve this
        url = self._url_for_path(path)
        r = requests.get(url)
        return ZincIndex.from_bytes(r.content, mutable=False)

    def get_manifest(self, bundle_name, version):
        path = self.id + '/' + bundle_name + '/' + str(version)  # TODO: improve this
        url = self._url_for_path(path)
        r = requests.get(url)
        return ZincManifest.from_bytes(r.content, mutable=False)

    def update_bundle(self, bundle_name, filelist, skip_master_archive=False,
                      force=False):
        # TODO: implement flags
        path = self.id + '/' + bundle_name + '/'  # TODO: improve this
        url = self._url_for_path(path)
        data = {'files': filelist.to_bytes()}
        requests.post(url, data=data)

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        path = self.id + '/' + bundle_name + '/tags/' + distribution_name
        url = self._url_for_path(path)
        data = {'version': int(bundle_version)}
        r = requests.put(url, data=data)
        # TODO: real error
        if r.status_code / 100 != 2:
            raise Exception("didn't work!")

    def delete_distribution(self, distribution_name, bundle_name):
        path = self.id + '/' + bundle_name + '/tags/' + distribution_name
        url = self._url_for_path(path)
        r = requests.delete(url)
        # TODO: real error
        if r.status_code / 100 != 2:
            raise Exception("didn't work!")


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
