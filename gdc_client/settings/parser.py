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
    parser.add_argument('--config', help=HELP, metavar='FILE')
    choices = parser.add_subparsers(title='Settings to display', dest='section')
    choices.required = True

    download_choice = choices.add_parser('download', help='Display download settings')
    download_choice.add_argument('--config', help=HELP, metavar='FILE')
    download_choice.set_defaults(func=partial(resolve, config_file))

    upload_choice = choices.add_parser('upload', help='Display upload settings')
    upload_choice.add_argument('--config', help=HELP, metavar='FILE')
    upload_choice.set_defaults(func=partial(resolve, config_file))
