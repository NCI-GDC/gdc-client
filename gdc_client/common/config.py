import argparse
import logging
import sys
from configparser import ConfigParser, NoOptionError, NoSectionError

from gdc_client.defaults import (
    processes, USER_DEFAULT_CONFIG_LOCATION, HTTP_CHUNK_SIZE, SAVE_INTERVAL,
    UPLOAD_PART_SIZE
)

log = logging.getLogger('gdc-client')

# This will display the default configs in a INI-type format, so that users
# will be able to copy and modify as needed
DISPLAY_TEMPLATE = '[{}]\n{}\n'


class GDCClientArgumentParser(argparse.ArgumentParser):
    """This is a workaround introduced here https://groups.google.com/forum/#!topic/argparse-users/LazV_tEQvQw
    which enables to print the full help message in case something went wrong
    with argument parsing
    """
    def error(self, message):
        self.print_help(sys.stderr)
        sys.stderr.write('\ngdc-client error: {}\n'.format(message))
        sys.exit(2)


class GDCClientConfigShared(object):
    setting_getters = {
        'server': ConfigParser.get,
        'http_chunk_size': ConfigParser.getint,
        'upload_part_size': ConfigParser.getint,
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
                'n_processes': processes,
            },
            'download': {
                'dir': '.',
                'save_interval': SAVE_INTERVAL,
                'http_chunk_size': HTTP_CHUNK_SIZE,
                'no_segment_md5sum': False,
                'no_file_md5sum': False,
                'no_verify': False,
                'no_related_files': False,
                'no_annotations': False,
                'no_auto_retry': False,
                'retry_amount': 1,
                'wait_time': 5.0,
            },
            'upload': {
                'path': '.',
                'upload_part_size': UPLOAD_PART_SIZE,
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

        return DISPLAY_TEMPLATE.format(
            section,
            '\n'.join(' = '.join([key, str(val)])
            for key, val in _config.items())
        )
