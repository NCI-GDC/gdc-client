from StringIO import StringIO
import os
import requests
import tarfile
import time
import urlparse

from parcel import HTTPClient, UDTClient, utils
from parcel.download_stream import DownloadStream
from ..query.index import GDCIndexClient

import logging

log = logging.getLogger('gdc-download')

class GDCDownloadMixin(object):

    def download_small_groups(self, smalls):
        # List[List[str]] -> List[List[str]]
        """Smalls are predetermined groupings of smaller filesize files.
        They are grouped to reduce the number of open connections per download

        """

        tarfile_url = self.data_uri + '?tarfile'
        errors = []
        groupings_len = len(smalls)
        for i, s in enumerate(smalls):
            if len(s) == 0:
                log.error('There are no files to download')
                return

            try:
                # post request
                # {'ids': ['id1', 'id2'..., 'idn']}
                ids = {"ids": s}

                # using a POST request lets us avoid the MAX URL character length limit
                r = requests.post(tarfile_url, stream=True, verify=self.verify, json=ids)
                if r.status_code == requests.codes.ok:

                    # {'content-disposition': 'filename=the_actual_filename.tar'}j
                    filename = r.headers.get('content-disposition') or \
                            r.headers.get('Content-Disposition')

                    if filename:
                        filename = os.path.join(self.directory, filename.split('=')[1])
                    else:
                        filename = time.strftime("gdc-client-%Y%m%d-%H%M%S")
                    log.info('Saving grouping {}/{}'.format(i+1, groupings_len))
                    with open(filename, 'wb') as f:
                        for chunk in r:
                            f.write(chunk)
                else:
                    log.warning('[{}] unable to download group {} '.format(r.status_code, i+1))
                    errors.append(ids['ids'])
                    time.sleep(0.5)

                r.close()

            except Exception as e:
                log.warning('Grouping download failed: {}'.format(i+1))
                errors.append(ids['ids'])
                log.warn(e)

        return errors


    def parallel_download(self, stream, download_related_files=None,
                          download_annotations=None, *args, **kwargs):

        # This is a little confusing because gdc-client
        # calls parcel's parallel_download, which is where
        # most of the downloading takes place
        stream.directory = self.directory
        super(GDCDownloadMixin, self).parallel_download(stream)

    def fix_url(self, url):
        """ Fix a url to be used in the rest of the program

            example:
                api.gdc.cancer.gov -> https://api.gdc.cancer.gov/
        """
        if not url.endswith('/'):
            url = '{}/'.format(url)

        if not (url.startswith('https://') or url.startswith('http://')):
            url = 'https://{}'.format(url)

        return url

class GDCHTTPDownloadClient(GDCDownloadMixin, HTTPClient):

    def __init__(self, uri, download_related_files=True,
                 download_annotations=True, *args, **kwargs):

        self.base_uri = self.fix_url(uri)
        self.data_uri = urlparse.urljoin(self.base_uri, 'v0/data/')
        self.related_files = download_related_files
        self.annotations = download_annotations

        self.directory = os.path.abspath(time.strftime("gdc-client-%Y%m%d-%H%M%S"))
        if kwargs.get('directory'):
            self.directory = kwargs.get('directory')

        self.verify = kwargs.get('verify')
        super(GDCDownloadMixin, self).__init__(self.data_uri, *args, **kwargs)


class GDCUDTDownloadClient(GDCDownloadMixin, UDTClient):

    def __init__(self, remote_uri, download_related_files=True,
                 download_annotations=True, *args, **kwargs):

        remote_uri = self.fix_url(remote_uri)
        self.base_uri = remote_uri
        self.data_uri = urlparse.urljoin(remote_uri, 'data/')
        self.related_files = download_related_files
        self.annotations = download_annotations
        self.directory = os.path.abspath(time.strftime("gdc-client-%Y%m%d-%H%M%S"))
        super(GDCDownloadMixin, self).__init__(*args, **kwargs)
