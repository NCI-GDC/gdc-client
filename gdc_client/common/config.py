from configparser import ConfigParser, NoOptionError, NoSectionError

from gdc_client import defaults


class GDCClientConfig(object):
    flag_getters = {
        'server': 'get',
        'http_chunk_size': 'getint',
        'save_interval': 'getint'
    }

    def __init__(self, config_path=defaults.CONFIG_DEFAULTS_LOCATION):
        self.sections = ['COMMON']
        self.config = ConfigParser()
        self.config.read(config_path)

    def to_dict(self):
        return {
            flag: self.get_setting(section, flag)
            for section in self.sections
            for flag in self.flag_getters
            if self.get_setting(section, flag) is not None
        }

    def get_setting(self, section, option):
        try:
            func = getattr(self.config, self.flag_getters[option])
            return func(section, option)
        except (NoOptionError, NoSectionError, KeyError):
            return None

    @property
    def display_string(self):
        _options = self.to_dict()

        return '\n'.join(' = '.join([str(key), str(val)])
                         for key, val in _options.items())
