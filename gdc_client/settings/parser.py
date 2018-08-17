import logging
from functools import partial

from gdc_client.common.config import (
    GDCClientUploadConfig, GDCClientDownloadConfig, GDCClientConfig
)


log = logging.getLogger('gdc-settings')


class SettingsResolver(object):
    def __init__(self, setting_cls=GDCClientConfig):
        self.config = setting_cls()

    def ls(self, args):
        log.info(self.config.display_string)
        return self.config.display_string

    def list(self, args):
        return self.ls(args)


def resolve(setting_cls, args):
    resolver = SettingsResolver(setting_cls)
    command = args.command
    func = getattr(resolver, command)
    return func(args)


def config(parser):
    sub_parsers = parser.add_subparsers(
        title='sub commands', dest='sub'
    )

    upload_resolver = partial(resolve, GDCClientUploadConfig)
    upload = sub_parsers.add_parser('upload')
    upload.add_argument('command', choices=['list', 'ls'])
    upload.set_defaults(func=upload_resolver)

    download_resolver = partial(resolve, GDCClientDownloadConfig)
    download = sub_parsers.add_parser('download')
    download.set_defaults(func=download_resolver)
    download.add_argument('command', choices=['list', 'ls'])
