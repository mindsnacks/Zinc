import os, json, urlparse, time, hashlib, zlib
import requests
import zinc

from boto.s3.connection import S3Connection
from redis_lock import Lock
from redis import Redis
from rq import Queue
from flask import Flask, request, redirect, abort

from config import ZINC_CONFIG

REDIS = Redis.from_url(os.environ.get('REDISTOGO_URL', 'redis://localhost:6379'))

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

def process_files(manifest):
	for path, info in manifest.files.items():
		format, _ = info.get('formats').items()[0]
		r = requests.get(object_url(manifest.catalog_id, info.get('sha') + ('.' + format if format != 'raw' else '')))

		bin = r.content
		if format == 'gz':
			bin = zlib.decompress(bin, 16+zlib.MAX_WBITS)
		sha = hashlib.sha1(bin)
		if sha.hexdigest() != info.get('sha'):
			return False

		info['bin'] = bin
	return True

def build_tar(manifest):
	for path, info in manifest.files.items():
		logger.warning(info.get('bin')[0:10])

# main queue job
def process_manifest(manifest):
	zindex = get_zinc_index(catalog)
	next_version = zindex.next_version_for_bundle(bundle)
	zindex.add_version_for_bundle(bundle, next_version)

	manifest = zinc.ZincManifest(catalog, bundle, next_version)
	manifest.files = request.json['files']
	manifest.determine_flavors_from_files()

	# verify files
	if not process_files(manifest):	
		abort(400, 'Bad file.')

	# generate tar
	build_tar(manifest)

	time.sleep(1)


app = Flask(__name__)
@app.route('/')
def root():
    return 'Metazinc!'

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

		# TODO: this could break if it takes longer than the expire time.
		with Lock(catalog + ':' + bundle, redis=REDIS, expires=60):
			# -- critical section (get and set)
			zindex = get_zinc_index(catalog)
			next_version = zindex.next_version_for_bundle(bundle)
			zindex.add_version_for_bundle(bundle, next_version)

			manifest = zinc.ZincManifest(catalog, bundle, next_version)
			manifest.files = request.json['files']
			manifest.determine_flavors_from_files()

			# verify files
			if not process_files(manifest):	
				abort(400, 'Bad file.')

			# generate tar
			build_tar(manifest)

			time.sleep(1)
			app.logger.warning('here')
			# generate and upload tars to /archives
			## s3 = S3Connection('<aws access key>', '<aws secret key>')
			# upload manifest
			# update catalog index
			# -- end critical section

		return json.dumps(manifest.to_json())

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)