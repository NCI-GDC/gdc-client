import sys
import argparse
import logging
import requests

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
                       type=argparse.FileType('r'),
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
                       action='store_true',
                       help='Print stack traces')


def main():
    args = subparser.parse_args(sys.argv[2:])
    if args.verbose:
        logging.root.setLevel(logging.DEBUG)
    print("Uploading file {} to project {} from path {}"
          .format(args.project_id, args.identifier, args.file))

    try:
        tokens = args.project_id.split('-')
        program = (tokens[0]).upper()
        project = ('-'.join(tokens[1:])).upper()
    except Exception as e:
        raise RuntimeError('Unable to parse project id {}: {}'
                           .format(args.project_id), e)

    url = args.server + '{}/{}/files/{}'.format(
        program, project, args.identifier)
    print("Attempting to upload to {}".format(url))

    headers = {'x-auth-token': args.token.read()}
    r = requests.post(url, data=args.file, headers=headers)
    print r.text
