import argparse
import logging

from functools import partial

from .. import defaults

from . import manifest
from . import exceptions

from .client import GDCUploadClient
from .. import log

logger = log.get_logger('upload-client')

def validate_args(parser, args):
    """ Validate argparse namespace.
    """

    if args.identifier:
        logger.warn('The use of the -i/--identifier flag has been deprecated.')

    if not args.token_file:
        parser.error('A token is required in order to upload.')

    if args.manifest or args.file_ids:
        return

    parser.error('must specify either --manifest or file_id(s)')


def upload(parser, args):
    """ Upload data to the GDC.
    """
    validate_args(parser, args)

    files = manifest.load(args.manifest)['files'] if args.manifest else []

    if not args.manifest:
        for uuid in args.file_ids:
            files.append({
                'id': args.identifier,
                'project_id': args.project_id,
                'path': args.path,
                'upload_id': args.upload_id,
            })

    # TODO remove debug - handled by logger
    debug = logging.DEBUG in args.log_levels

    manifest_name = args.manifest.name if args.manifest else args.file_ids[0]

    client = GDCUploadClient(
        token=args.token_file, processes=args.n_processes,
        multipart=args.disable_multipart,
        part_size=args.part_size, server=args.server,
        files=files, verify=args.insecure, debug=debug, manifest_name=manifest_name)

    if args.abort:
        client.abort()
    elif args.delete:
        client.delete()
    else:
        client.upload()

def config(parser):
    """ Configure a parser for upload.
    """
    func = partial(upload, parser)
    parser.set_defaults(func=func)

    parser.add_argument('--project-id', '-p', type=str,
                        help='The project ID that owns the file')
    parser.add_argument('--path', '-f', metavar='path',
                        help='directory path to find file')
    parser.add_argument('--upload-id', '-u',
                        help='Multipart upload id')

    parser.add_argument('--insecure', '-k',
                        action='store_false',
                        help='Allow connections to server without certs')
    # TODO remove this and replace w/ top level host and port
    parser.add_argument('--server', '-s',
                        default=defaults.tcp_url,
                        help='GDC API server address')
    parser.add_argument('--part-size', '-ps',
                        default=defaults.part_size,
                        type=int,
                        help='Part size for multipart upload')
    parser.add_argument('-n', '--n-processes', type=int,
                        default=defaults.processes,
                        help='Number of client connections')
    parser.add_argument('--disable-multipart',
                        action="store_false",
                        help='Disable multipart upload')
    parser.add_argument('--abort',
                        action="store_true",
                        help='Abort previous multipart upload')
    parser.add_argument('--resume', '-r',
                        action="store_true",
                        help='Resume previous multipart upload')
    parser.add_argument('--delete',
                        action="store_true",
                        help='Delete an uploaded file')

    parser.add_argument('--manifest', '-m',
                        type=argparse.FileType('r'),
                        help='Manifest which describes files to be uploaded')
    parser.add_argument('--identifier', '-i', action='store_true',
                        help='DEPRECATED')
    parser.add_argument('file_ids',
                        metavar='file_id', type=str,
                        nargs='*',
                        help='The GDC UUID of the file(s) to upload')

