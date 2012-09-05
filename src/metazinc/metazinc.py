import tornadoredis
import tornado.web
import tornado.gen
import json

#from zinc import *
#from zinc.models import bundle_id_from_bundle_descriptor, bundle_version_from_bundle_descriptor

redis = tornadoredis.Client()
redis.connect()

def bundle_key(repo, bundle):
    return "%s:%s" % (repo, bundle)

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

class RepoHandler(tornado.web.RequestHandler):
    def get(self, repo):
        self.write("repo: %s" % repo)
        #self.finish()

class BundleHandler(tornado.web.RequestHandler):
    @async_with_gen
    def get(self, repo, bundle):
        key = bundle_key(repo, bundle)
        locked = yield tornado.gen.Task(redis.smembers, key)
        self.write(json.dumps(list(locked)))
        self.finish()

    @async_with_gen
    def post(self, repo, bundle):
        key = bundle_key(repo, bundle)

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
    def put(self, repo, bundle):
        key = bundle_key(repo, bundle)
        version = self.get_argument('version')
        res = yield tornado.gen.Task(redis.srem, key, version)
        if res > 0:
            redis.publish(key + ":lock", "unlocked")
            self.write('Unlocked bundle: %s' % bundle)
        else:
            self.write('No lock found')
        self.finish()



class LockHandler(tornado.web.RequestHandler):
    @async_with_gen
    def get(self, repo, bundle):
        self.key = bundle_key(repo, bundle)
        self.client = tornadoredis.Client()
        self.client.connect()
        locked = yield tornado.gen.Task(redis.scard, self.key) # size of set
        if locked > 0:
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
            yield tornado.gen.Task(self.client.unsubscribe, self.key + ":lock")
            self.client.disconnect()
        else:
            self.write('...\n')

application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/([a-z0-9.]+)/?", RepoHandler),
    (r"/([a-z0-9.]+)/([\w-]+)", BundleHandler),
    (r"/([a-z0-9.]+)/([\w-]+)/lock", LockHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()