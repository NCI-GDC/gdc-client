import argparse


def config(parser):
    """ Configure argparse parser for GDC auth token parsing.
    """
    token_group = parser.add_mutually_exclusive_group()

    token_group.add_argument('-t', '--token',
        # TODO add type check for token format
        help='GDC API auth token string',
    )

    token_group.add_argument('--token-file',
        type=argparse.FileType('rb'),
        help='GDC API auth token file',
    )
