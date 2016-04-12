import argparse
import logging

from .. import defaults

from . import manifest
from . import exceptions

from .client import GDCUploadClient


def upload(args):
    """ Upload data to the GDC.
    """
    files = manifest.load(args.manifest)['files'] if args.manifest else []
    files.append({
        'id': args.identifier,
        'project_id': args.project_id,
        'path': args.path,
        'upload_id': args.upload_id,
    })

    # TODO remove debug - handled by logger
    debug = logging.DEBUG in args.log_levels

    manifest_name = args.manifest.name if args.manifest else args.identifier

    client = GDCUploadClient(
        token=args.token, processes=args.n_processes,
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
    parser.set_defaults(func=upload)

    parser.add_argument('--project-id', '-p', type=str,
                        help='The project ID that owns the file')
    parser.add_argument('--identifier', '-i', type=str,
                        help='The file id')
    parser.add_argument('--path', '-f', metavar='path',
                        help='directory path to find file')
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
    parser.add_argument('--upload-id', '-u',
                        help='Multipart upload id')
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
