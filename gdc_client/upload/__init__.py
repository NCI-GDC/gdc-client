from .. import defaults
import sys
import argparse
import logging
import requests
from client import GDCUploadClient, GDCMultipartUploadClient
from ..argparser import subparsers

command = 'upload'
subparser = subparsers.add_parser(command)


subparser.add_argument('--project-id', '-p', type=str,
                       required=True,
                       help='The project ID that owns the file')
subparser.add_argument('--identifier', '-i', type=str,
                       required=True,
                       help='The id or alias')
subparser.add_argument('--file', '-f', metavar='file',
                       required=True,
                       help='file to upload')
subparser.add_argument('--token', '-t', metavar='file',
                       required=True,
                       type=argparse.FileType('r'),
                       help='auth token')
subparser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Print stack traces')
subparser.add_argument('--server', '-s',
                       default='http://localhost:8080/v0/submission/',
                       help='GDC API server address')
subparser.add_argument('--part-size', '-ps',
                       default='5000000',
                       type=int,
                       help='Part size for multipart upload')
subparser.add_argument('-n', '--n-processes', type=int,
                       default=defaults.processes,
                       help='Number of client connections.')
subparser.add_argument('--multipart-upload', '-m',
                       action="store_true",
                       help='Use multipart upload')
subparser.add_argument('--abort', '-a',
                       action="store_true",
                       help='Abort previous multipart upload')


def main():
    args = subparser.parse_args(sys.argv[2:])
    if args.verbose:
        logging.root.setLevel(logging.DEBUG)
    print("Uploading file {} to project {} from path {}"
          .format(args.identifier, args.project_id, args.file))

    try:
        tokens = args.project_id.split('-')
        program = (tokens[0]).upper()
        project = ('-'.join(tokens[1:])).upper()
    except Exception as e:
        raise RuntimeError('Unable to parse project id {}: {}'
                           .format(args.project_id), e)
    url = args.server + '{}/{}/files/{}'.format(
        program, project, args.identifier)
    client = GDCUploadClient(
      url, args.token.read(), args.file, args.n_processes,
      multipart=args.multipart_upload,
      part_size=args.part_size)
    if args.abort:
      client.abort()

    client.upload()
    
