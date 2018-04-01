import logging
import time
import urlparse

from functools import partial
from parcel import const
from parcel import colored
from parcel import manifest

from gdc_client import defaults
from gdc_client.download import GDCHTTPDownloadClient
from gdc_client.query import GDCIndexClient

LOG = logging.getLogger('gdc-download')

UDT_SUPPORT = ' '.join([
    'UDT is supported through the use of the Parcel UDT proxy.',
    'To set up a Parcel UDT proxy for use with the GDC client,',
    'please contact the GDC Help Desk at support@nci-gdc.datacommons.io.',
])

def validate_args(parser, args):
    """Validate argparse namespace."""
    if not args.file_ids and not args.manifest:
        msg = 'must specify either --manifest or file_id'
        parser.error(msg)

    if args.udt:
        # We were asked to remove 'error' in the message
        parser.exit(status=1, message=UDT_SUPPORT)

def get_client(args, index_client):
    # args get converted into kwargs
    kwargs = {
        'uri': args.server,
        'index_client': index_client,
        'token': args.token_file,
        'n_procs': args.n_processes,
        'directory': args.dir,
        'segment_md5sums': args.segment_md5sums,
        'file_md5sum': args.file_md5sum,
        'http_chunk_size': args.http_chunk_size,
        'save_interval': args.save_interval,
        'download_related_files': args.download_related_files,
        'download_annotations': args.download_annotations,
        'no_auto_retry': args.no_auto_retry,
        'retry_amount': args.retry_amount,
        'verify': not args.no_verify,
    }
    # The option to use UDT should be hidden until
    # (1) the external library is packaged into the binary and
    # (2) the GDC supports Parcel servers in production
    # if args.udt:
    #     server = args.server or defaults.udt_url
    #     return GDCUDTDownloadClient(
    #         remote_uri=server,
    #         proxy_host=args.proxy_host,
    #         proxy_port=args.proxy_port,
    #         external_proxy=args.external_proxy,
    #         **kwargs
    #     )
    # else:
    return GDCHTTPDownloadClient(**kwargs)

def download(parser, args):
    """Downloads data from the GDC.

    Combine the smaller files (~KB range) into a grouped download.
    The API now supports combining UUID's into one uncompressed tarfile
    using the ?tarfile url parameter. Combining many smaller files into one
    download decreases the number of open connections we have to make
    """

    successful_count = 0
    unsuccessful_count = 0
    big_errors = []
    small_errors = []
    total_download_count = 0
    validate_args(parser, args)

    # sets do not allow duplicates in a list
    ids = set(args.file_ids)
    for i in args.manifest:
        if not i.get('id'):
            LOG.error('Invalid manifest')
            break
        ids.add(i['id'])

    index_client = GDCIndexClient(args.server)
    client = get_client(args, index_client)

    # separate the smaller files from the larger files
    bigs, smalls = index_client.separate_small_files(ids, args.http_chunk_size)

    # the big files will be normal downloads
    # the small files will be joined together and tarfiled
    if smalls:
        LOG.debug('Downloading smaller files...')

        # download small file grouped in an uncompressed tarfile
        small_errors, count = client.download_small_groups(smalls)
        successful_count += count

        i = 0
        while i < args.retry_amount and small_errors:
            time.sleep(args.wait_time)
            LOG.debug('Retrying failed grouped downloads')
            small_errors, count = client.download_small_groups(small_errors)
            successful_count += count
            i += 1

    # client.download_files is located in parcel which calls
    # self.parallel_download, which goes back to to gdc-client's parallel_download
    if bigs:
        LOG.debug('Downloading big files...')

        # create URLs to send to parcel for download
        bigs = [urlparse.urljoin(client.data_uri, b) for b in bigs]
        downloaded_files, big_error_dict = client.download_files(bigs)
        not_downloaded_url = ''
        big_errors_count = 0

        if args.retry_amount > 0:
            for url, reason in big_error_dict.iteritems():
                # only retry the download if it wasn't a controlled access error
                if '403' not in reason:
                    not_downloaded_url = retry_download(
                        client,
                        url,
                        args.retry_amount,
                        args.no_auto_retry,
                        args.wait_time,
                    )
                else:
                    big_errors.append(url)
                    not_downloaded_url = ''

                if not_downloaded_url:
                    for b in big_error_dict:
                        big_errors.append(url)

        if big_errors:
            big_error_string = ', '.join([b.split('/')[-1] for b in big_errors])
            LOG.debug('Big files not downloaded: %s', big_error_string)

        successful_count += len(bigs) - len(big_errors)

    unsuccessful_count = len(ids) - successful_count

    LOG.info('%s: %d',
             colored('Successfully downloaded', 'green'),
             successful_count)

    if unsuccessful_count > 0:
        LOG.info('%s: %d',
                 colored('Failed downloads', 'red'),
                 unsuccessful_count)

    return small_errors or big_errors


def retry_download(client, url, retry_amount, no_auto_retry, wait_time):

    LOG.debug('Retrying download %s', url)

    error = True
    while retry_amount > 0 and error:
        if no_auto_retry:
            should_retry = raw_input('Retry download for {0}? (y/N): '.format(url))
        else:
            should_retry = 'y'

        if should_retry.lower() == 'y':
            LOG.debug('%d retries remaining...', retry_amount)
            LOG.debug('Retrying download... %s in %d seconds', url, wait_time)
            retry_amount -= 1
            time.sleep(wait_time)
            # client.download_files accepts a list of urls to download
            # but we want to only try one at a time
            _, e = client.download_files([url])
            if not e:
                LOG.debug('Successfully downloaded %s!', url)
                return
        else:
            error = False
            retry_amount = 0

    LOG.error('Unable to download file %s', url)
    return url


def config(parser):
    """Configure a parser for download."""
    func = partial(download, parser)
    parser.set_defaults(func=func)

    #############################################################
    #                     General options
    #############################################################

    parser.add_argument('-d', '--dir', default='.',
                        help='Directory to download files to. '
                        'Defaults to current dir')
    parser.add_argument('-s', '--server', metavar='server', type=str,
                        default=defaults.tcp_url,
                        help='The TCP server address server[:port]')
    parser.add_argument('--no-segment-md5sums', dest='segment_md5sums',
                        action='store_false',
                        help='Do not calculate inbound segment md5sums '
                        'and/or do not verify md5sums on restart')
    parser.add_argument('--no-file-md5sum', dest='file_md5sum',
                        action='store_false',
                        help='Do not verify file md5sum after download')
    parser.add_argument('-n', '--n-processes', type=int,
                        default=defaults.processes,
                        help='Number of client connections.')
    parser.add_argument('--http-chunk-size', '-c', type=int,
                        default=const.HTTP_CHUNK_SIZE,
                        help='Size in bytes of standard HTTP block size.')
    parser.add_argument('--save-interval', type=int,
                        default=const.SAVE_INTERVAL,
                        help='The number of chunks after which to flush state '
                        'file. A lower save interval will result in more '
                        'frequent printout but lower performance.')
    parser.add_argument('--no-verify', dest='no_verify', action='store_true',
                        help='Perform insecure SSL connection and transfer')
    parser.add_argument('--no-related-files', action='store_false',
                        dest='download_related_files',
                        help='Do not download related files.')
    parser.add_argument('--no-annotations', action='store_false',
                        dest='download_annotations',
                        help='Do not download annotations.')
    parser.add_argument('--no-auto-retry', action='store_true',
                        dest='no_auto_retry',
                        help='Ask before retrying to download a file')
    parser.add_argument('--retry-amount', default=1,
                        dest='retry_amount',
                        help='Number of times to retry a download')
    parser.add_argument('--wait-time', default=5.0,
                        dest='wait_time', type=float,
                        help='Amount of seconds to wait before retrying')

    #############################################################
    #                       UDT options
    #############################################################

    # The option to use UDT should be hidden until
    # (1) the external library is packaged into the binary and
    # (2) the GDC supports Parcel servers in production
    parser.add_argument('-u', '--udt', action='store_true',
                        help='Use the UDT protocol.')

    # parser.add_argument('--proxy-host', default=defaults.proxy_host,
    #                     type=str, dest='proxy_host',
    #                     help='The port to bind the local proxy to')
    # parser.add_argument('--proxy-port', default=defaults.proxy_port,
    #                     type=str, dest='proxy_port',
    #                     help='The port to bind the local proxy to')
    # parser.add_argument('-e', '--external-proxy', action='store_true',
    #                     dest='external_proxy',
    #                     help='Do not create a local proxy but bind to an external one')
    parser.add_argument(
        '-m',
        '--manifest',
        type=manifest.argparse_type,
        default=[],
        help='GDC download manifest file',
    )
    parser.add_argument(
        'file_ids',
        metavar='file_id',
        nargs='*',
        help='The GDC UUID of the file(s) to download',
    )
