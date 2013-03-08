import os
import json
import base64
import hmac, hashlib

from redis import Redis
from rq import Queue
from flask import Flask, request, abort, make_response, Response
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

Coordinator = RedisCatalogCoordinator(redis=REDIS)
Q = Queue(connection=REDIS)
Jobs = dict()
Catalogs = dict()

app = Flask(__name__) # todo 

def get_catalog(catalog_id):
    catalog = Catalogs.get(catalog_id)
    if catalog is None:
        storage = S3StorageBackend(
                s3connection=S3,
                bucket=CONFIG['s3_bucket'],
                prefix=catalog_id)
        catalog = ZincCatalog(coordinator=Coordinator, storage=storage)
        Catalogs[catalog_id] = catalog
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

        catalog = get_catalog(catalog_id)
       
        filelist = request.form['files']
        force = True # hardcode force for now
        
        job = Q.enqueue(catalog.update_bundle, bundle, filelist, force=force)
        Jobs[job.id] = job

        return job.id

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

#{"expiration": "2009-01-01T00:00:00Z",
#  "conditions": [ 
#    {"bucket": "s3-bucket"}, 
#    ["starts-with", "$key", "uploads/"],
#    {"acl": "private"},
#    {"success_action_redirect": "http://localhost/"},
#    ["starts-with", "$Content-Type", ""],
#    ["content-length-range", 0, 1048576]
#  ]
#}

def upload_key(catalog_id, sha):
    return catalog_id + '/uploads/' + sha

def build_policy(catalog_id, sha):
    policy = dict()
    policy['expiration'] = "2014-01-01T00:00:00Z"
    policy['conditions'] = [
            {'bucket' : CONFIG['s3_bucket']},
            {'key' : upload_key(catalog_id, sha)},
            #{'acl' : 'private'}
            ]
    return policy

def base64_policy(policy):
    return base64.b64encode(json.dumps(policy))

def sign_policy(policy64):
    signature = base64.b64encode(
            hmac.new(CONFIG['aws_secret'], policy64, hashlib.sha1).digest())
    return signature


@app.route('/<catalog_id>/files/<sha>/upload', methods=['POST'])
def upload(catalog_id, sha):
    catalog = get_catalog(catalog_id)
    info = catalog._get_file_info(sha)
    if info is not None:
        abort(409, 'File exists')
        return

    policy = build_policy(catalog_id, sha)
    policy64 = base64_policy(policy)
    signature = sign_policy(policy64)

    response_data = dict()
    response_data['policy'] = policy64
    response_data['signature'] = signature
    response_data['AWSAccessKeyId'] = CONFIG['aws_key']
    response_data['key'] = upload_key(catalog_id, sha)

    js = json.dumps(response_data)
    resp = Response(js, status=200, mimetype='application/json')
    return resp

#@app.route('/<catalog_id>/upload', methods=['POST'])
#    catalog = get_catalog(catalog_id)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
