import logging
from ConfigParser import ConfigParser, NoOptionError, NoSectionError

from gdc_client import defaults


log = logging.getLogger('gdc-client-config')

GB = 1024 * 1024 * 1024


class GDCClientConfig(object):
    setting_getters = {
        'server': ConfigParser.get,
        'http_chunk_size': ConfigParser.getint,
        'save_interval': ConfigParser.getint,
    }

    def __init__(self, config_path=None):
        # The order of configs determine their priority
        # DEFAULT < USER DEFAULT < CUSTOM
        log.info('user defaults: {}'.format(defaults.USER_DEFAULT_CONFIG_LOCATION))
        configs = [
            defaults.USER_DEFAULT_CONFIG_LOCATION
        ]

        if config_path is not None:
            configs.append(config_path)

        self.sections = ['common']
        self.config = ConfigParser()
        self.config.read(configs)

    def to_dict(self):
        config_settings = {
            option: self.get_setting(section, option)
            for section in self.sections for option in self.setting_getters
            if self.get_setting(section, option) is not None
        }
        defaults = self.defaults
        defaults.update(config_settings)

        return defaults

    @property
    def defaults(self):
        return {
            'server': 'https://api.gdc.cancer.gov',
            'http_chunk_size': 1 * GB,
            'save_interval': 1 * GB,
        }

    def get_setting(self, section, option):
        try:
            return self.setting_getters[option](self.config, section, option)
        except NoOptionError:
            log.debug('Setting named "{}" not found in section "{}"'.format(
                option, section))
        except NoSectionError:
            log.debug('No section named "{}" found'.format(section))
        except KeyError:
            log.debug('Invalid setting "{}"'.format(option))
        return None

    @property
    def display_string(self):
        _config = self.to_dict()

        return '\n'.join(' = '.join([key, str(val)])
                         for key, val in _config.items())


class GDCClientDownloadConfig(GDCClientConfig):
    def __init__(self, config_path=None):
        super(GDCClientDownloadConfig, self).__init__(config_path)
        self.sections.append('download')
        self.setting_getters.update({
            'dir': ConfigParser.get,
            'server': ConfigParser.get,
            'n_processes': ConfigParser.getint,
            'retry_amount': ConfigParser.getint,
            'wait_time': ConfigParser.getfloat,
            'no_segment_md5sums': ConfigParser.getboolean,
            'no_file_md5sum': ConfigParser.getboolean,
            'no_verify': ConfigParser.getboolean,
            'no_related_files': ConfigParser.getboolean,
            'no_annotations': ConfigParser.getboolean,
            'no_auto_retry': ConfigParser.getboolean,
        })

    def to_dict(self):
        _config = super(GDCClientDownloadConfig, self).to_dict()
        _config['n_processes'] = defaults.processes
        _config['manifest'] = []

        return _config

    @property
    def defaults(self):
        defaults = super(GDCClientDownloadConfig, self).defaults

        download_defaults = {
            'dir': '.',
            'no_segment_md5sums': False,
            'no_file_md5sum': False,
            'no_verify': False,
            'no_related_files': False,
            'no_annotations': False,
            'no_auto_retry': False,
            'retry_amount': 1,
            'wait_time': 5.0,
        }
        defaults.update(download_defaults)

        return defaults


class GDCClientUploadConfig(GDCClientConfig):
    def __init__(self, config_path=None):
        super(GDCClientUploadConfig, self).__init__(config_path)

        self.sections.append('upload')
        self.setting_getters.update({
            'insecure': ConfigParser.getboolean,
            'disable_multipart': ConfigParser.getboolean,
            'path': ConfigParser.get,
        })

    def to_dict(self):
        _config = super(GDCClientUploadConfig, self).to_dict()
        _config['n_processes'] = defaults.processes

        return _config

    @property
    def defaults(self):
        defaults = super(GDCClientUploadConfig, self).defaults

        upload_defaults = {
            'path': ".",
            'insecure': False,
            'disable_multipart': False,
        }
        defaults.update(upload_defaults)

        return defaults
