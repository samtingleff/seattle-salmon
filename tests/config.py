
class TestConfig(object):
    configs = {
        'bdb':{
            'data-dir':'/home/sam/src/seattle-salmon/tmp/data',
            'home-dir':'/home/sam/src/seattle-salmon/tmp/home',
            'log-dir':'/home/sam/src/seattle-salmon/tmp/logs',
            'splits':8}
    }

    def get_option(self, category, name, default=None):
        try: val = TestConfig.configs[category][name]
        except KeyError: val = default
        return val

    def get_int_option(self, category, name, default=0):
        return int(self.get_option(category, name, default=default))

