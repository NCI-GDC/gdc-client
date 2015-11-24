import argparse
import sys
import logging

from . import client
from ..repl import run_repl
from .. import defaults
from ..argparser import subparsers
from parcel import manifest, const

command = 'download'
subparser = subparsers.add_parser(command)

#############################################################
#                     General options
#############################################################

subparser.add_argument('-m', '--manifest',
                       type=manifest.argparse_type,
                       default=list(),
                       help='GDC Download manifest file.')
subparser.add_argument('-v', '--verbose', action='store_true',
                       help='verbose logging')
subparser.add_argument('-d', '--dir',
                       default=None,
                       help='Directory to download files to. '
                       'Defaults to current dir')
subparser.add_argument('-s', '--server', metavar='server', type=str,
                       default=None,
                       help='The UDT server address server[:port]')
subparser.add_argument('file_ids', metavar='file_id', type=str,
                       nargs='*', help='uuids to download')
subparser.add_argument('--no-segment-md5sums', dest='segment_md5sums',
                       action='store_false',
                       help='Calculate inbound segment md5sums and/or verify md5sums on restart')
subparser.add_argument('--debug', dest='debug',
                       action='store_true',
                       help='Print stack traces')
subparser.add_argument('-n', '--n-processes', type=int,
                       default=defaults.processes,
                       help='Number of client connections.')
subparser.add_argument('--http-chunk-size', type=int,
                       default=const.HTTP_CHUNK_SIZE,
                       help='Size in bytes of standard HTTP block size.')
subparser.add_argument('--save-interval', type=int,
                       default=const.SAVE_INTERVAL,
                       help='The number of chunks after which to flush state file. A lower save interval will result in more frequent printout but lower performance.')
subparser.add_argument('--no-related-files', action='store_false',
                       dest='download_related_files',
                       help='Do not download related files.')
subparser.add_argument('--no-annotations', action='store_false',
                       dest='download_annotations',
                       help='Do not download annotations.')

token_args = subparser.add_mutually_exclusive_group(required=False)
token_args.add_argument('-t', '--token-file',
                        type=lambda x: argparse.FileType('r')(x).read(),
                        dest='token',
                        help='authentication token file')
token_args.add_argument('-T', '--token', default='', type=str,
                        dest='token', help='authentication token')

#############################################################
#                       UDT options
#############################################################

subparser.add_argument('-u', '--udt', action='store_true',
                       help='Use the UDT protocol.  Better for WAN connections')
subparser.add_argument('-H', '--proxy-host', default=defaults.proxy_host,
                       type=str, dest='proxy_host',
                       help='The port to bind the local proxy to')
subparser.add_argument('-P', '--proxy-port', default=defaults.proxy_port,
                       type=str, dest='proxy_port',
                       help='The port to bind the local proxy to')
subparser.add_argument('-e', '--external-proxy', action='store_true',
                       dest='external_proxy',
                       help='Do not create a local proxy but bind to an external one')


def get_client(args, token, **_kwargs):
    kwargs = {
        'token': token,
        'n_procs': args.n_processes,
        'directory': args.dir,
        'segment_md5sums': args.segment_md5sums,
        'debug': args.debug,
        'http_chunk_size': args.http_chunk_size,
        'save_interval': args.save_interval,
        'download_related_files': args.download_related_files,
        'download_annotations': args.download_annotations,
    }

    if args.udt:
        server = args.server or defaults.udt_url
        return client.GDCUDTDownloadClient(
            remote_uri=server,
            proxy_host=args.proxy_host,
            proxy_port=args.proxy_port,
            external_proxy=args.external_proxy,
            **kwargs
        )
    else:
        server = args.server or defaults.tcp_url
        return client.GDCHTTPDownloadClient(
            uri=server,
            **kwargs
        )


def run_cli(args):
    client = get_client(args, args.token)

    # Exclude the first argument which will be `download.command`
    # and add ids from manifest
    file_ids = set([f['id'] for f in args.manifest] + args.file_ids[1:])
    client.download_files(file_ids)


def main():
    args = subparser.parse_args()
    if args.verbose:
        logging.root.setLevel(logging.DEBUG)

    # If there are arguments other than subcommand, run cli
    if sys.argv[2:]:
        try:
            run_cli(args)
        except Exception as e:
            if args.debug:
                raise
            else:
                print('Process aborted: {}'.format(str(e)))

    # Else, run as a repl
    else:
        run_repl(args)
