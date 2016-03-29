import argparse

from parcel import const

from .. import defaults

from .client import GDCUDTDownloadClient
from .client import GDCHTTPDownloadClient

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
        return GDCUDTDownloadClient(
            remote_uri=server,
            proxy_host=args.proxy_host,
            proxy_port=args.proxy_port,
            external_proxy=args.external_proxy,
            **kwargs
        )
    else:
        server = args.server or defaults.tcp_url
        return GDCHTTPDownloadClient(
            uri=server,
            **kwargs
        )

def parse_manifest(fs):
    """ Parse a GDC manifest from a file-like object.

    :param fs:
        File-like object containing a GDC manifest.
    """
    manifest = yaml.load(fs)

    return (entity for entity in manifest)

def download(args):
    """ Downloads data from the GDC.
    """
    client = get_client(args, args.token)

    file_ids = set(args.file_ids)
    for f in args.manifest:
        file_ids.add(f['id'])
   
    if not len(file_ids):
        print "***ERROR: Download requires either a list of file ids or a manifest file."
        print "          Please use '-h' for further help."
    else:
        client.download_files(file_ids)

def config(parser):
    """ Configure a parser for download.
    """
    parser.set_defaults(func=download)

    #############################################################
    #                     General options
    #############################################################

    parser.add_argument('-m', '--manifest',
                        type=lambda x: parse_manifest(argparse.FileType('r')(x)),
                        default=list(),
                        help='GDC Download manifest file.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose logging')
    parser.add_argument('-d', '--dir',
                        default=None,
                        help='Directory to download files to. '
                        'Defaults to current dir')
    parser.add_argument('-s', '--server', metavar='server', type=str,
                        default=None,
                        help='The UDT server address server[:port]')
    parser.add_argument('file_ids', metavar='file_id', type=str,
                        nargs='*', help='uuids to download')
    parser.add_argument('--no-segment-md5sums', dest='segment_md5sums',
                        action='store_false',
                        help='Calculate inbound segment md5sums and/or verify md5sums on restart')
    parser.add_argument('--debug', dest='debug',
                        action='store_true',
                        help='Print stack traces')
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

    token_args = parser.add_mutually_exclusive_group(required=False)
    token_args.add_argument('-t', '--token-file',
                            type=lambda x: argparse.FileType('r')(x).read(),
                            dest='token',
                            help='authentication token file')
    token_args.add_argument('-T', '--token', default='', type=str,
                            dest='token', help='authentication token')

    #############################################################
    #                       UDT options
    #############################################################

    parser.add_argument('-u', '--udt', action='store_true',
                        help='Use the UDT protocol.  Better for WAN connections')
    parser.add_argument('-H', '--proxy-host', default=defaults.proxy_host,
                        type=str, dest='proxy_host',
                        help='The port to bind the local proxy to')
    parser.add_argument('-P', '--proxy-port', default=defaults.proxy_port,
                        type=str, dest='proxy_port',
                        help='The port to bind the local proxy to')
    parser.add_argument('-e', '--external-proxy', action='store_true',
                        dest='external_proxy',
                        help='Do not create a local proxy but bind to an external one')
