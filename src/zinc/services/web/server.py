import os

from redis import Redis
#from rq import Queue
from flask import Flask, request, abort, make_response
from boto.s3.connection import S3Connection

from zinc.catalog import ZincCatalogPathHelper
from zinc.services import ZincCatalog
from zinc.coordinators.redis import RedisCatalogCoordinator
from zinc.storages.aws import S3StorageBackend

# TODO: temp
from config import CONFIG

API_VERSION = '1.0'
REDIS_URL = os.environ.get('REDISTOGO_URL', 'redis://localhost:6379')
REDIS = Redis.from_url(REDIS_URL)
S3 = S3Connection(CONFIG['aws_key'], CONFIG['aws_secret'])

coordinator = RedisCatalogCoordinator(redis=REDIS)

app = Flask(__name__) # todo 

catalogs = dict()

def get_catalog(catalog_id):
    catalog = catalogs.get(catalog_id)
    if catalog is None:
        storage = S3StorageBackend(
                s3connection=S3,
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

@app.route('/<catalog_id>/<bundle>/tags/<tag>', methods=['PUT', 'DELETE'])
def tag(catalog_id, bundle, tag):

    if request.method == 'PUT':
        catalog = get_catalog(catalog_id)
        version = int(request.form['version'])
        catalog.update_distribution(tag, bundle, version)

    if request.method == 'DELETE':
        catalog = get_catalog(catalog_id)
        catalog.delete_distribution(tag, bundle)
    
    response = make_response()
    response.status_code = 200
    return response

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
