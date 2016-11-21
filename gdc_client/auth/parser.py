import os
import stat
from .. import log as logger
import argparse
import platform

from contextlib import closing


PLATFORM_HELP = {
    'Darwin': 'On OS X: chmod 600 {token_file}',
    'Linux': 'On Linux: chmod 600 {token_file}',
}

PLATFORM_HELP_DEFAULT = 'Contact your system administrator for assistance.'

PERMISSIONS_MSG = ' '.join([
    'Your token file \'{token_file}\' is not properly secured.',
    'Please secure your token file by ensuring that it is not readable',
    'or writeable by anyone other than the owner of the file.',
    PLATFORM_HELP.get(platform.system(), PLATFORM_HELP_DEFAULT),
])

def read_token_file(path):
    """ Safely open, read and close a token file.
    """
    log = logger.get_logger('gdc-client')

    # TODO review best way to check file security on various platforms

    abspath = os.path.abspath(path)
    try:
        stats = os.stat(abspath)
    except OSError as err:
        raise argparse.ArgumentTypeError(err)

    invalid_permissions = any([
        stats.st_mode & stat.S_IRWXG, # Group can R/W/X.
        stats.st_mode & stat.S_IRWXO, # Other can R/W/X.
    ])

    if invalid_permissions:
        permissions_msg = PERMISSIONS_MSG.format(
            token_file=abspath,
        )
        log.warn(permissions_msg)
        # FIXME convert to error after investigation on windows
        #raise argparse.ArgumentTypeError(permissions_msg)

    try:
        ifs = open(abspath, 'r')
    except IOError as err:
        raise argparse.ArgumentTypeError(err)

    with closing(ifs):
        return ifs.read().strip()


def config(parser):
    """ Configure argparse parser for GDC auth token parsing.
    """

    parser.add_argument('-t', '--token-file',
        type=read_token_file,
        help='GDC API auth token file',
    )
