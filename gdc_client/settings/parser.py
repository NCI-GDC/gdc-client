import logging
from functools import partial

from gdc_client.common.config import GDCClientConfigShared

logger = logging.getLogger('gdc-client')

HELP = (
    'Path to INI-type config file. See what settings will look like if a custom'
    ' config file is used'
)


class SettingsResolver(object):
    def __init__(self, config_file):
        self.config = GDCClientConfigShared(config_file)

    def download(self):
        logger.info(self.config.to_display_string('download'))
        return self.config.to_display_string('download')

    def upload(self):
        logger.info(self.config.to_display_string('upload'))
        return self.config.to_display_string('upload')


def resolve(config_file, args):
    resolver = SettingsResolver(config_file)
    func = getattr(resolver, args.section)
    return func()


def config(parser, config_file=None):
    parser.add_argument('section', choices=['download', 'upload'])
    parser.add_argument('--config', help=HELP, metavar='FILE')

    resolver = partial(resolve, config_file)
    parser.set_defaults(func=resolver)
