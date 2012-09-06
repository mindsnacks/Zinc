import tornadoredis
import tornado.web
import tornado.gen
import os, urlparse, json

#from zinc import *
#from zinc.models import bundle_id_from_bundle_descriptor, bundle_version_from_bundle_descriptor

r_url = urlparse.urlparse(os.environ.get('REDISTOGO_URL', 'redis://localhost:6379'))
redis = tornadoredis.Client(host=r_url.hostname, port=r_url.port, password=r_url.password)
redis.connect()

def bundle_key(catalog, bundle):
    return "%s:%s" % (catalog, bundle)

# simpler decorator
def async_with_gen(fn):
    return tornado.web.asynchronous(tornado.gen.engine(fn))

class MainHandler(tornado.web.RequestHandler):
    @async_with_gen
    def get(self):
        foo = yield tornado.gen.Task(redis.get, 'foo')
        self.set_header('Content-Type', 'text/html')
        self.write("foo is %s" % foo)
        self.finish()

    @async_with_gen
    def post(self):
    	foo = self.get_argument('foo')
    	res = yield tornado.gen.Task(redis.set, 'foo', foo)
    	self.write('foo set')
        self.finish()

class CatalogHandler(tornado.web.RequestHandler):
    def get(self, catalog):
        self.write("catalog: %s" % catalog)
        #self.finish()

class BundleHandler(tornado.web.RequestHandler):
    @async_with_gen
    def get(self, catalog, bundle):
        key = bundle_key(catalog, bundle)
        locked = yield tornado.gen.Task(redis.smembers, key)
        self.write(json.dumps(list(locked)))
        self.finish()

    @async_with_gen
    def post(self, catalog, bundle):
        key = bundle_key(catalog, bundle)

        # TODO: this get/add should be a transaction
        locked = yield tornado.gen.Task(redis.scard, key) # size of set
        if locked > 0:
            self.send_error(409) # HTTP Conflict
            return

        # TODO: figure out the actual latest version
        version = 1
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
        self.client = tornadoredis.Client()
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
    (r"/([a-z0-9.]+)/([\w-]+)", BundleHandler),
    (r"/([a-z0-9.]+)/([\w-]+)/lock", LockHandler),
])

if __name__ == "__main__":
    application.listen(os.environ.get('PORT', 8888))
    tornado.ioloop.IOLoop.instance().start()