import os
import stat
import logging
import argparse

from contextlib import closing


PERMISSIONS_MSG = '''
Your token file {token_file} is not properly secured. Please secure your
token file by ensuring that it is not readable or writeable by anyone
other than the owner of the file.

On OS X: <os_x_instructions>
On Linux: <linux_instructions>
On Windows: <windows_instructions>
'''

def read_token_file(path):
    """ Safely open, read and close a token file.
    """
    log = logging.getLogger('gdc-client')

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
    token_group = parser.add_mutually_exclusive_group()

    token_group.add_argument('-T', '--token',
        # TODO add type check for token format
        default=os.environ.get('GDC_AUTH_TOKEN'),
        help='GDC API auth token string',
    )

    token_group.add_argument('-t', '--token-file',
        dest='token',
        type=read_token_file,
        help='GDC API auth token file',
    )
