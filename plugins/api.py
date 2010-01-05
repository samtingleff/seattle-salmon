import thread

"""
Send an http post of one of these to http://host:port/each/ to run on each
key/value pair.
"""
class IKeyValueIterator(object):
    def configure(self, storage): pass

    def item(self, key, value): pass

    def close(self): pass

"""
These are loaded from plugins/load.
"""
class IStoragePlugin(object):
    def __init__(self): pass

    def name(self): pass

"""
Responsible for the plugin registry.
"""
class PluginManager(object):
    __lockObj = thread.allocate_lock()
    __instance = None

    def __new__(cls, *args, **kargs):
        cls.__lockObj.acquire()
        try:
            if cls.__instance is None: cls.__instance = object.__new__(cls, *args, **kargs)
        finally:
            cls.__lockObj.release()
        return cls.__instance

    def __init__(self): pass

    def load(self, imports_file):
        self.callbacks = {}
        try:
            fh = open(imports_file, "r")
        except TypeError: return
        for line in fh:
            line = line.strip()
            if line.startswith("#") or line == "": continue
            __import__(line, globals(), locals(), [])

    def register(self, event_name, func):
        try:
            self.callbacks[event_name].append(func)
        except KeyError: self.callbacks[event_name] = [func]

    def execute(self, event_name, func, *args, **kargs):
        fn, plugins = func, None
        try:
            plugins = self.callbacks[event_name]
        except KeyError: pass
        if plugins:
            for plugin in plugins:
                result = plugin(fn, *args, **kargs)
                if hasattr(result, '__call__'): fn = result
        if fn: return fn(*args, **kargs)

