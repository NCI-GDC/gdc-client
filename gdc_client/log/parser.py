import argparse
import logging
import sys

try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client

from .. import version


FORMAT = '%(asctime)s: %(levelname)s: %(message)s'

def setup_logging(args):
    """ Set up logging given parsed logging arguments.
    """
    logging.basicConfig(
        format=FORMAT,
        level=min(args.log_levels),
        stream=args.log_file,
    )

    http_client.HTTPConnection.debuglevel = args.trace

def config(parser):
    """ Configure an argparse parser for logging.
    """
    parser.set_defaults(log_levels=[logging.ERROR])

    parser.add_argument('--trace',
        action='store_true',
        help='enable requests tracing',
    )

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
