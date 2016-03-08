import os
import stat
import argparse

from contextlib import closing


def read_token_file(path):
    """ Safely open, read and close a token file.
    """
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
        raise argparse.ArgumentTypeError('token file is not secured')

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

    token_group.add_argument('-t', '--token',
        # TODO add type check for token format
        default=os.environ.get('GDC_AUTH_TOKEN'),
        help='GDC API auth token string',
    )

    token_group.add_argument('--token-file',
        dest='token',
        type=read_token_file,
        help='GDC API auth token file',
    )
