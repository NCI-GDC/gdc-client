import logging
from functools import partial

from gdc_client.common.config import GDCClientConfigShared


log = logging.getLogger('gdc-settings')

HELP = (
    'Path to INI-type config file. See what settings will look like if a custom'
    ' config file is used'
)


class SettingsResolver(object):
    def __init__(self, config_file):
        self.config = GDCClientConfigShared(config_file)

    def ls(self, section, args):
        log.info(self.config.to_display_string(section))
        return self.config.to_display_string(section)

    def show(self, section, args):
        return self.ls(section, args)


def resolve(section, config_file, args):
    resolver = SettingsResolver(config_file)
    command = args.command
    func = getattr(resolver, command)
    return func(section, args)


def config(parser, config_file=None):
    sub_parsers = parser.add_subparsers(
        title='sub commands', dest='sub'
    )

    upload_resolver = partial(resolve, 'upload', config_file)
    upload = sub_parsers.add_parser('upload')
    upload.add_argument('command', choices=['show', 'ls'])
    upload.add_argument('--config', help=HELP)
    upload.set_defaults(func=upload_resolver)

    download_resolver = partial(resolve, 'download', config_file)
    download = sub_parsers.add_parser('download')
    download.add_argument('command', choices=['show', 'ls'])
    download.add_argument('--config', help=HELP)
    download.set_defaults(func=download_resolver)
