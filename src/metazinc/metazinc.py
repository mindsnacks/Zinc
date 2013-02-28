import os, json, urlparse, time, hashlib, zlib, tempfile, tarfile
import requests
import zinc

from boto.s3.connection import S3Connection
from redis_lock import Lock
from redis import Redis
from rq import Queue
from flask import Flask, request, redirect, abort

from config import ZINC_CONFIG

REDIS = Redis.from_url(os.environ.get('REDISTOGO_URL', 'redis://localhost:6379'))
API_VERSION = '1.0'

def catalog_index_url(catalog):
	return ZINC_CONFIG["url"] + '/' + catalog + '/index.json'
def manifest_url(catalog, bundle, version):
	return ZINC_CONFIG["url"] + '/' + catalog + '/manifests/' + bundle + '-' + str(version) + '.json'
def object_url(catalog, sha):
	return ZINC_CONFIG["url"] + '/' + catalog + '/objects/' + sha[0:2] + '/' + sha[2:4] + '/' + sha

def get_zinc_index(catalog):
	r = requests.get(catalog_index_url(catalog))
	return zinc.ZincIndex.from_dict(r.json())

def valid_manifest(req):
	manifest = req.json
	# simple sanity checks
	if not manifest or \
	   not manifest.has_key('files') or \
	   not (lambda fs: len(fs.keys()) and \
	   		(lambda f: f.has_key('sha') and \
	   				   f.has_key('formats'))(fs[fs.keys()[0]]))(manifest['files']):
		return False
	return True

def file_path(manifest, path):
	return manifest.catalog_id + "/" + manifest.bundle_name + "/" + path

def format_from_info(file_info):
	return file_info.get('formats').items()[0][0]

def file_extension(file_info):
	format = format_from_info(file_info)
	return ('.' + format if format != 'raw' else '')

def process_files(manifest):
	for path, info in manifest.files.items():
		format = format_from_info(info)
		r = requests.get(object_url(manifest.catalog_id, info.get('sha') + file_extension(info)))

		info['bin'] = bin = r.content
		if format == 'gz':
			bin = zlib.decompress(bin, 16+zlib.MAX_WBITS)
		sha = hashlib.sha1(bin)
		if sha.hexdigest() != info.get('sha'):
			return False

	return True

def build_tars(manifest):
	files = {}

	tar = tarfile.open(manifest.bundle_name + '-' + str(manifest.version) + '.tar')
	for path, info in manifest.files.items():
		temp = tempfile.NamedTemporaryFile()
		temp.write(info.get('bin'))
		format = format_from_info(info)
		tar.add(nametemp.name, arcname=info.get('sha') + file_extension(info))


# main queue job
def process_manifest(manifest):
	zindex = get_zinc_index(catalog)
	next_version = zindex.next_version_for_bundle(bundle)
	zindex.add_version_for_bundle(bundle, next_version)

	manifest.files = request.json['files']
	manifest.determine_flavors_from_files()

	# verify files
	if not process_files(manifest):	
		abort(400, 'Bad file.')

	# generate tar
	build_tars(manifest)

	return manifest

Q = Queue(connection=REDIS)
Jobs = {}

app = Flask(__name__)

@app.after_request
def after_request(response):
    response.headers.add('X-Zinc-API-Version', API_VERSION)
    return response

@app.route('/')
def root():
    return 'Metazinc!'

@app.route('/jobs/<id>')
def get_job(id):
	return Jobs[id].result

@app.route('/<catalog>')
@app.route('/<catalog>/index.json')
def catalog_index(catalog):
	return redirect(catalog_index_url(catalog))

@app.route('/<catalog>/<bundle>', methods=['GET', 'POST'])
@app.route('/<catalog>/<bundle>/<int:version>')
def manifest(catalog, bundle, version=None):
	if request.method == 'GET':
		if not version:
			version = get_zinc_index(catalog).versions_for_bundle(bundle)[-1]

		return redirect(manifest_url(catalog, bundle, version))

	if request.method == 'POST':
		if not valid_manifest(request):
			abort(400, 'Bad manifest.')
		
		manifest = zinc.ZincManifest(catalog, bundle, next_version)
		job = Q.enqueue(proccess_manifest, manifest)
		Jobs[job.id] = job

		return job.id

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
