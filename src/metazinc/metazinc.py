import os, json, urlparse
import requests
import zinc
from flask import Flask, request, redirect, abort

ZINC_CONFIG = json.loads(open('config.json').read())
REDIS = urlparse.urlparse(os.environ.get('REDISTOGO_URL', 'redis://localhost:6379'))

def catalog_index_url(catalog):
	return ZINC_CONFIG["url"] + '/' + catalog + '/index.json'
def manifest_url(catalog, bundle, version):
	return ZINC_CONFIG["url"] + '/' + catalog + '/manifests/' + bundle + '-' + str(version) + '.json'

def get_zinc_index(catalog):
	r = requests.get(catalog_index_url(catalog))
	return zinc.ZincIndex.from_dict(r.json())

def valid_manifest(req):
	manifest = req.json
	# simple sanity checks
	if not manifest or \
	   not manifest.has_key('files') or \
	   not len(manifest['files'].keys()) or \
	   not manifest['files'][manifest['files'].keys()[0]]['sha']:
		return False
	return True

 
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

		print manifest_url(catalog, bundle, version)
		return redirect(manifest_url(catalog, bundle, version))

	if request.method == 'POST':
		if not valid_manifest(request):
			abort(400)

		# -- critical section (get and set)
		zindex = get_zinc_index(catalog)
		next_version = zindex.next_version_for_bundle(bundle)
		zindex.add_version_for_bundle(bundle, next_version)

		manifest = zinc.ZincManifest(catalog, bundle, next_version)
		manifest.files = request.json['files']
		manifest.determine_flavors_from_files()

		# verify files
		# upload manifest
		# generate and upload tars to /archives
		# update catalog index
		# -- end critical section

		return json.dumps(manifest.to_json())

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)