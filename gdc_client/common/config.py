import logging
from configparser import ConfigParser, NoOptionError, NoSectionError

from gdc_client.defaults import (
    processes, USER_DEFAULT_CONFIG_LOCATION, HTTP_CHUNK_SIZE
)

log = logging.getLogger('gdc-client-config')


class GDCClientConfigShared(object):
    setting_getters = {
        'server': ConfigParser.get,
        'http_chunk_size': ConfigParser.getint,
        'save_interval': ConfigParser.getint,
        'dir': ConfigParser.get,
        'n_processes': ConfigParser.getint,
        'retry_amount': ConfigParser.getint,
        'wait_time': ConfigParser.getfloat,
        'no_segment_md5sum': ConfigParser.getboolean,
        'no_file_md5sum': ConfigParser.getboolean,
        'no_verify': ConfigParser.getboolean,
        'no_related_files': ConfigParser.getboolean,
        'no_annotations': ConfigParser.getboolean,
        'no_auto_retry': ConfigParser.getboolean,
        'insecure': ConfigParser.getboolean,
        'disable_multipart': ConfigParser.getboolean,
        'path': ConfigParser.get,
    }

    def __init__(self, config_path=None):
        configs = [USER_DEFAULT_CONFIG_LOCATION]

        if config_path is not None:
            configs.append(config_path)

        self.config = ConfigParser()
        self.config.read(configs)

    @property
    def defaults(self):
        return {
            'common': {
                'server': 'https://api.gdc.cancer.gov',
                'http_chunk_size': HTTP_CHUNK_SIZE,
                'save_interval': HTTP_CHUNK_SIZE,
                'n_processes': processes,
            },
            'download': {
                'dir': '.',
                'no_segment_md5sum': False,
                'no_file_md5sum': False,
                'no_verify': False,
                'no_related_files': False,
                'no_annotations': False,
                'no_auto_retry': False,
                'retry_amount': 1,
                'wait_time': 5.0,
                'manifest': [],
            },
            'upload': {
                'path': '.',
                'insecure': False,
                'disable_multipart': False,
            },
        }

    def to_dict(self, section):
        config_settings = {
            option: self.get_setting(section, option)
            for option in self.setting_getters
            if self.get_setting(section, option) is not None
        }

        defaults = self.defaults[section]
        defaults.update(config_settings)

        return defaults

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

        # Return defaults if nothing was provided in config file

        return (
            self.defaults[section].get(option) or
            self.defaults['common'].get(option)
        )

    def to_display_string(self, section):
        _config = self.to_dict(section)

        return '\n'.join(' = '.join([key, str(val)])
                         for key, val in list(_config.items()))
