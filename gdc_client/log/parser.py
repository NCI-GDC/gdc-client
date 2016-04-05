import argparse
import logging
import sys

from .. import version


def setup_logging(args):
    """ Set up logging given parsed logging arguments.
    """
    logging.root.setLevel(min(args.log_levels))

def config(parser):
    """ Configure an argparse parser for logging.
    """
    parser.set_defaults(log_levels=[logging.ERROR])

    parser.add_argument('--debug',
        action='append_const',
        dest='log_levels',
        const=logging.DEBUG,
        help='enable debug logging',
    )

    parser.add_argument('-v', '--verbose',
        action='append_const',
        dest='log_levels',
        const=logging.INFO,
        help='enable verbose logging',
    )

    parser.add_argument('--log-file',
        type=argparse.FileType('a'),
        default=sys.stderr,
        help='log file [stderr]',
    )
