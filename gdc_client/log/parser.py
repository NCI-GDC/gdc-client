import argparse
import log
import logging
import sys

from .. import version

from .log import LogFormatter


def setup_logging(args):
    """ Set up logging given parsed logging arguments.
    """
    logging.root.setLevel(min(args.log_levels))

    if args.log_file and args.log_file != sys.stderr:
        logger_filename = args.log_file.name

    if sys.stderr.isatty():
        formatter = LogFormatter()
    else:
        formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')


    logging.basicConfig(
            level=min(args.log_levels),
            filename=logger_filename,
            formatter=formatter)

    log.logger_filename = logger_filename

def config(parser):
    """ Configure an argparse parser for logging.
    """

    parser.set_defaults(log_levels=[logging.WARNING])

    parser.add_argument('--debug',
        action='append_const',
        dest='log_levels',
        const=logging.DEBUG,
        help='Enable debug logging. If a failure occurs, the program will stop.',
    )

    parser.add_argument('-v', '--verbose',
        action='append_const',
        dest='log_levels',
        const=logging.INFO,
        help='Enable verbose logging',
    )

    parser.add_argument('--log-file',
        dest='log_file',
        type=argparse.FileType('a'),
        default=sys.stderr,
        help='Save logs to file. Amount logged affected by --debug, and --verbose flags',
    )
