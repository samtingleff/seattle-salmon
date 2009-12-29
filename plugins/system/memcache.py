from plugins.api import IStoragePlugin, PluginManager

from cache import memcache

class MemcachePlugin(IStoragePlugin):
    def __init__(self):
        PluginManager().register("storage.get", self.get)
        PluginManager().register("storage.set", self.set)
        PluginManager().register("storage.delete", self.delete)
        PluginManager().register("system.configure", self.configure)
        PluginManager().register("system.start", self.start)

    def configure(self, func, config):
        self.hosts = config.get_option('cache', 'memcached-hosts', 'localhost:11211').split(', ')

    def start(self, func):
        self.mc = memcache.Client(self.hosts, debug=0)

    def get(self, func, key):
        val = self.mc.get(key)
        if val: return lambda k: val

    def set(self, func, key, value):
        self.mc.set(key, value)

    def delete(self, func, key):
        self.mc.delete(key)

MemcachePlugin()

