from configparser import ConfigParser, NoOptionError, NoSectionError

from gdc_client import defaults


class GDCClientConfig(object):
    setting_getters = {
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
            option: self.get_setting(section, option)
            for section in self.sections for option in self.setting_getters
            if self.get_setting(section, option) is not None
        }

    def get_setting(self, section, option):
        try:
            getter = getattr(self.config, self.setting_getters[option])
            return getter(section, option)
        except (NoOptionError, NoSectionError, KeyError):
            return None

    @property
    def display_string(self):
        _config = self.to_dict()

        return '\n'.join(' = '.join([str(key), str(val)])
                         for key, val in _config.items())


class GDCClientDownloadConfig(GDCClientConfig):
    def __init__(self, config_path=defaults.CONFIG_DEFAULTS_LOCATION):
        super(GDCClientDownloadConfig, self).__init__(config_path)
        self.sections.append('DOWNLOAD')
        self.setting_getters.update({
            'dir': 'get',
            'server': 'get',
            'n_processes': 'getint',
            'retry_amount': 'getint',
            'wait_time': 'getfloat',
            'no_segment_md5sums': 'getboolean',
            'no_file_md5sum': 'getboolean',
            'no_verify': 'getboolean',
            'no_related_files': 'getboolean',
            'no_annotations': 'getboolean',
            'no_auto_retry': 'getboolean'
        })

    def to_dict(self):
        _config = super(GDCClientDownloadConfig, self).to_dict()
        _config['n_processes'] = defaults.processes
        _config['manifest'] = []

        return _config


class GDCClientUploadConfig(GDCClientConfig):
    def __init__(self, config_path=defaults.CONFIG_DEFAULTS_LOCATION):
        super(GDCClientUploadConfig, self).__init__(config_path)

        self.sections.append('UPLOAD')
        self.setting_getters.update({
            'insecure': 'getboolean',
            'disable_multipart': 'getboolean',
            'path': 'get'
        })

    def to_dict(self):
        _config = super(GDCClientUploadConfig, self).to_dict()
        _config['n_processes'] = defaults.processes

        return _config
