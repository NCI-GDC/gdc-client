from .. import defaults
import sys
import argparse
import yaml
import logging
import requests
from client import GDCUploadClient
from ..argparser import subparsers

command = 'upload'
subparser = subparsers.add_parser(command)


subparser.add_argument('--project-id', '-p', type=str,
                       help='The project ID that owns the file')
subparser.add_argument('--identifier', '-i', type=str,
                       help='The id or alias')
subparser.add_argument('--file-path', '-f', metavar='file',
                       help='file to upload')
subparser.add_argument('--token', '-t', metavar='file',
                       required=True,
                       type=argparse.FileType('r'),
                       help='auth token')
subparser.add_argument('--insecure', '-k',
                       action='store_false',
                       help='Allow connections to server without certs')
subparser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Print stack traces')
subparser.add_argument('--server', '-s',
                       default=defaults.tcp_url,
                       help='GDC API server address')
subparser.add_argument('--part-size', '-ps',
                       default='5242880',
                       type=int,
                       help='Part size for multipart upload')
subparser.add_argument('-n', '--n-processes', type=int,
                       default=defaults.processes,
                       help='Number of client connections.')
subparser.add_argument('--upload-id', '-u',
                       help='Multipart upload id')
subparser.add_argument('--disable-multipart',
                       action="store_false",
                       help='Disable multipart upload')
subparser.add_argument('--abort',
                       action="store_true",
                       help='Abort previous multipart upload')
subparser.add_argument('--resume', '-r',
                       action="store_true",
                       help='Resume previous multipart upload')
subparser.add_argument('--delete',
                       action="store_true",
                       help='Delete an uploaded file')
subparser.add_argument('--manifest', '-m',
                       type=argparse.FileType('r'),
                       help='Manifest which describes files to be uploaded')


def main():
    args = subparser.parse_args(sys.argv[2:])
    if args.verbose:
        logging.root.setLevel(logging.DEBUG)

    files = read_manifest(yaml.load(args.manifest)) if args.manifest else\
        [{"id": args.identifier, "project_id": args.project_id,
          "path": args.file_path, "upload_id": args.upload_id}]
    client = GDCUploadClient(
        token=args.token.read(), processes=args.n_processes,
        multipart=args.disable_multipart,
        part_size=args.part_size, server=args.server,
        files=files, verify=args.insecure, debug=args.verbose)
    if args.abort:
        client.abort()
    elif args.delete:
        client.delete()
    else:
        client.upload()


def read_manifest(manifest):
    if type(manifest) == list:
        return sum([read_manifest(item) for item in manifest], [])
    if "files" in manifest:
        return manifest['files']
    else:
        return sum([read_manifest(item) for item in manifest.values()], [])
