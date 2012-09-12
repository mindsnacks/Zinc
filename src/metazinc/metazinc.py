import tornadoredis
import tornado.web
import tornado.gen
import tornado.httpclient
import os, urlparse, json

import zinc

r_url = urlparse.urlparse(os.environ.get('REDISTOGO_URL', 'redis://localhost:6379'))

def redisClient():
    client = tornadoredis.Client(host=r_url.hostname, port=r_url.port, password=r_url.password)
    client.connect()
    return client
redis = redisClient()

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

class BundleHandler(tornado.web.RequestHandler):
    def initialize(self, remote):
        self.remote = remote

    @tornado.gen.engine
    def catalog_index(self, catalog, callback=None):
        """Retrieves index file and invokes callback with the ZincIndex"""
        index_json = yield tornado.gen.Task(redis.get, catalog)
        if not index_json:
            http_client = tornado.httpclient.AsyncHTTPClient()
            url =  '%s/%s/index.json' % (self.remote['url'], catalog)
            print url
            index_json = yield tornado.gen.Task(http_client.fetch, url)
            redis.set(catalog, index_json, callback=lambda x: x or self.send_error())
        index = zinc.ZincIndex.from_dict(json.loads(index_json))
        callback(index)

    @async_with_gen
    def get(self, catalog, bundle):
        key = bundle_key(catalog, bundle)
        locked = yield tornado.gen.Task(redis.smembers, key)
        self.write(json.dumps(list(locked)))
        self.finish()

    @async_with_gen
    def post(self, catalog, bundle):

        key = bundle_key(catalog, bundle)

        self.client = redisClient()

        yield tornado.gen.Task(self.client.watch, key)
        locked = yield tornado.gen.Task(self.client.scard, key) # size of set

        # Currently, we lock all bundle versions during a client upload.
        # If a second client tries to get a lock, they'll get a 409 and will
        # be expected to wait on a /lock. After which, they should re-sync
        # the bundle so that it is up to date before attempting an upload.
        #
        # In the future, we could allow different versions to be uploaded,
        # simultaneously, if we keep track of what is changing in bundles.
        if locked > 0:
            self.send_error(409) # HTTP Conflict
            return

        index = yield tornado.gen.Task(self.catalog_index, catalog)
        version = index.versions_for_bundle(bundle)[-1] + 1

        transaction = self.client.pipeline(transactional=True)
        transaction.sadd(key, version)
        res = yield tornado.gen.Task(transaction.execute)

        if res is None:
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
            index = yield tornado.gen.Task(self.catalog_index, catalog)
            index.add_version_for_bundle(bundle, version)
            client = redisClient()
            yield tornado.gen.Task(client.watch, catalog)

            self.write('Unlocked bundle: %s' % bundle)
            redis.publish(key + ":lock", "unlocked")
        else:
            self.write('No lock found')
        self.finish()

    def on_finish(self):
        if hasattr(self, 'client'):
            self.client.disconnect()



class LockHandler(tornado.web.RequestHandler):
    @async_with_gen
    def get(self, catalog, bundle):
        self.key = bundle_key(catalog, bundle)
        self.client = redisClient()
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
        else:
            self.write('...\n')
            self.flush()

    def on_finish(self):
        if hasattr(self, 'client'):
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