import argparse
import logging

from functools import partial

from parcel import const
from parcel import manifest

from .. import defaults
from .. import log as logger

from .client import GDCUDTDownloadClient
from .client import GDCHTTPDownloadClient


UDT_SUPPORT = ' '.join([
    'UDT is supported through the use of the Parcel UDT proxy.',
    'To set up a Parcel UDT proxy for use with the GDC client,',
    'please contact the GDC Help Desk at support@nci-gdc.datacommons.io.',
])

def validate_args(parser, args):
    """ Validate argparse namespace.
    """
    if not args.file_ids and not args.manifest:
        msg = 'must specify either --manifest or file_id'
        parser.error(msg)

    if args.udt:
        # We were asked to remove 'error' in the message
        parser.exit(status=1, message=UDT_SUPPORT)

def get_client(args, token, **_kwargs):
    kwargs = {
        'token': token,
        'n_procs': args.n_processes,
        'directory': args.dir,
        'segment_md5sums': args.segment_md5sums,
        'file_md5sum': args.file_md5sum,
        # TODO remove debug argument - handled by logger
        'debug': logging.DEBUG in args.log_levels,
        'http_chunk_size': args.http_chunk_size,
        'save_interval': args.save_interval,
        'download_related_files': args.download_related_files,
        'download_annotations': args.download_annotations,
    }
    # The option to use UDT should be hidden until
    # (1) the external library is packaged into the binary and
    # (2) the GDC supports Parcel servers in production
    '''
    if args.udt:
        server = args.server or defaults.udt_url
        return GDCUDTDownloadClient(
            remote_uri=server,
            proxy_host=args.proxy_host,
            proxy_port=args.proxy_port,
            external_proxy=args.external_proxy,
            **kwargs
        )
    else:
    '''
    server = args.server or defaults.tcp_url
    return GDCHTTPDownloadClient(
        uri=server,
        **kwargs
    )

def download(parser, args):
    """ Downloads data from the GDC.
    """
    validate_args(parser, args)

    ids = set(args.file_ids)
    for i in args.manifest:
        ids.add(i['id'])

    client = get_client(args, args.token_file)
    client.download_files(ids)

def config(parser):
    """ Configure a parser for download.
    """
    func = partial(download, parser)
    parser.set_defaults(func=func)

    #############################################################
    #                     General options
    #############################################################

    parser.add_argument('-d', '--dir',
                        default=None,
                        help='Directory to download files to. '
                        'Defaults to current dir')
    parser.add_argument('-s', '--server', metavar='server', type=str,
                        default=None,
                        help='The TCP server address server[:port]')
    parser.add_argument('--no-segment-md5sums', dest='segment_md5sums',
                        action='store_false',
                        help='Calculate inbound segment md5sums and/or verify md5sums on restart')
    parser.add_argument('--no-file-md5sum', dest='file_md5sum',
                        action='store_false',
                        help="Don't verify file md5sum after download")
    parser.add_argument('-n', '--n-processes', type=int,
                        default=defaults.processes,
                        help='Number of client connections.')
    parser.add_argument('--http-chunk-size', type=int,
                        default=const.HTTP_CHUNK_SIZE,
                        help='Size in bytes of standard HTTP block size.')
    parser.add_argument('--save-interval', type=int,
                        default=const.SAVE_INTERVAL,
                        help='The number of chunks after which to flush state file. A lower save interval will result in more frequent printout but lower performance.')
    parser.add_argument('--no-related-files', action='store_false',
                        dest='download_related_files',
                        help='Do not download related files.')
    parser.add_argument('--no-annotations', action='store_false',
                        dest='download_annotations',
                        help='Do not download annotations.')

    #############################################################
    #                       UDT options
    #############################################################

    # The option to use UDT should be hidden until
    # (1) the external library is packaged into the binary and
    # (2) the GDC supports Parcel servers in production
    parser.add_argument('-u', '--udt', action='store_true',
                        help='Use the UDT protocol.')
    '''
    parser.add_argument('--proxy-host', default=defaults.proxy_host,
                        type=str, dest='proxy_host',
                        help='The port to bind the local proxy to')
    parser.add_argument('--proxy-port', default=defaults.proxy_port,
                        type=str, dest='proxy_port',
                        help='The port to bind the local proxy to')
    parser.add_argument('-e', '--external-proxy', action='store_true',
                        dest='external_proxy',
                        help='Do not create a local proxy but bind to an external one')
    '''
    parser.add_argument('-m', '--manifest',
        type=manifest.argparse_type,
        default=[],
        help='GDC download manifest file',
    )
    parser.add_argument('file_ids',
        metavar='file_id',
        nargs='*',
        help='GDC files to download',
    )
