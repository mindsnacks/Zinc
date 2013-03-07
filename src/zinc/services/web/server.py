import os
from urlparse import urljoin

from redis import Redis
#from rq import Queue
from flask import Flask, request, redirect, abort

from zinc.catalog import ZincCatalogPathHelper
from zinc.services import ZincCatalog
from zinc.coordinators.redis import RedisCatalogCoordinator
from zinc.storages.aws import S3StorageBackend

# TODO: temp
from config import CONFIG

API_VERSION = '1.0'
REDIS_URL = os.environ.get('REDISTOGO_URL', 'redis://localhost:6379')
REDIS = Redis.from_url(REDIS_URL)

coordinator = RedisCatalogCoordinator(redis=REDIS)

app = Flask(__name__) # todo 

catalogs = dict()

def get_catalog(catalog_id):
    catalog = catalogs.get(catalog_id)
    if catalog is None:
        storage = S3StorageBackend(
                key=CONFIG['aws_key'], 
                secret=CONFIG['aws_secret'],
                bucket=CONFIG['s3_bucket'],
                prefix=catalog_id)
        catalog = ZincCatalog(coordinator=coordinator, storage=storage)
        catalogs[catalog_id] = catalog
    return catalog

@app.after_request
def after_request(response):
    response.headers.add('X-Zinc-API-Version', API_VERSION)
    return response

@app.route('/<catalog_id>')
@app.route('/<catalog_id>/index.json')
def catalog_index(catalog_id):
    return get_catalog(catalog_id).get_index().to_bytes()

@app.route('/<catalog_id>/<bundle>', methods=['GET', 'POST'])
@app.route('/<catalog_id>/<bundle>/<int:version>')
def manifest(catalog_id, bundle, version=None):
    if request.method == 'GET':
        return get_catalog(catalog_id).get_manifest(bundle, version).to_bytes()

    if request.method == 'POST':

        raise NotImplementedError()

        #if not valid_manifest(request):
        #   abort(400, 'Bad manifest.')
        #
        #manifest = zinc.ZincManifest(catalog, bundle, next_version)
        #job = Q.enqueue(proccess_manifest, manifest)
        #Jobs[job.id] = job

        #return job.id

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
