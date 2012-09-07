import tornadoredis
import tornado.web
import tornado.gen
import tornado.httpclient
import os, urlparse, json

import zinc

r_url = urlparse.urlparse(os.environ.get('REDISTOGO_URL', 'redis://localhost:6379'))
def redisClient():
    return tornadoredis.Client(host=r_url.hostname, port=r_url.port, password=r_url.password)
redis = redisClient()
redis.connect()

ZINC_CONFIG = json.loads(open('config.json').read())

def bundle_key(catalog, bundle):
    return "%s:%s" % (catalog, bundle)

# simpler decorator
def async_with_gen(fn):
    return tornado.web.asynchronous(tornado.gen.engine(fn))

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("metazinc")

class CatalogHandler(tornado.web.RequestHandler):
    def get(self, catalog):
        self.write("catalog: %s" % catalog)
        #self.finish()

class BundleHandler(tornado.web.RequestHandler):
    def initialize(self, remote):
        self.remote = remote
        self.manifests = {}

    def manifest(self, catalog, callback=None):
        def cache_index(response):
            self.manifests[catalog] = zinc.ZincIndex.from_dict(json.loads(response.body))
            callback(self.manifests[catalog])

        if self.manifests.has_key(catalog):
            callback(self.manifests[catalog])
        else:
            http_client = tornado.httpclient.AsyncHTTPClient()
            url =  '%s/%s/index.json' % (self.remote['url'], catalog)
            print url
            resp =  http_client.fetch(url, cache_index)


    @async_with_gen
    def get(self, catalog, bundle):
        key = bundle_key(catalog, bundle)
        locked = yield tornado.gen.Task(redis.smembers, key)
        self.write(json.dumps(list(locked)))
        self.finish()

    @async_with_gen
    def post(self, catalog, bundle):
        manifest = yield tornado.gen.Task(self.manifest, catalog)

        key = bundle_key(catalog, bundle)

        # TODO: this entire get/add operation should be a transaction
        locked = yield tornado.gen.Task(redis.scard, key) # size of set

        # Currently, we lock all bundle versions during a client upload.
        # If a second client tries to get a lock, they'll get a 409 and will
        # be expected to wait on a /lock. After which, they should re-sync
        # the bundle so that it is up to date before attempting a upload.
        #
        # In the future, we could allow different versions to be uploaded,
        # simultaneously, if we keep track of what is changing in bundles.
        if locked > 0:
            self.send_error(409) # HTTP Conflict
            return

        version = manifest.versions_for_bundle(bundle)[-1] + 1
        manifest.add_version_for_bundle(bundle, version)
        res = yield tornado.gen.Task(redis.sadd, key, version)

        if res != version:
            self.send_error() # redis error
            return

        self.write(json.dumps(version))
        self.finish()

    @async_with_gen
    def put(self, catalog, bundle):
        key = bundle_key(catalog, bundle)
        version = self.get_argument('version')
        res = yield tornado.gen.Task(redis.srem, key, version)
        if res:
            self.write('Unlocked bundle: %s' % bundle)
            redis.publish(key + ":lock", "unlocked")
        else:
            self.write('No lock found')
        self.finish()



class LockHandler(tornado.web.RequestHandler):
    @async_with_gen
    def get(self, catalog, bundle):
        self.key = bundle_key(catalog, bundle)
        self.client = redisClient()
        self.client.connect()
        locked = yield tornado.gen.Task(redis.scard, self.key) # size of set
        if locked > 0:
            self.write('Waiting...\n')
            # send buffer over network immediately so client doesn't think we're unresponsive
            self.flush()
            yield tornado.gen.Task(self.client.subscribe, self.key + ":lock")

            self.client.listen(self.lock_watch)
        else:
            self.write('No locks')
            self.finish()

    def lock_watch(self, msg):
        if msg.kind == 'message' and msg.body == 'unlocked':
            self.write('Unlocked!')
            self.finish()
            
            # cleanup
            self.on_connection_close()
        else:
            self.write('...\n')
            self.flush()

    def on_connection_close(self):
        self.client.unsubscribe(self.key + ":lock")
        self.client.disconnect()

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/([a-z0-9.]+)/?", CatalogHandler),
    (r"/([a-z0-9.]+)/([\w-]+)", BundleHandler, dict(remote=ZINC_CONFIG['remote'])),
    (r"/([a-z0-9.]+)/([\w-]+)/lock", LockHandler),
])

if __name__ == "__main__":
    application.listen(os.environ.get('PORT', 8888))
    tornado.ioloop.IOLoop.instance().start()