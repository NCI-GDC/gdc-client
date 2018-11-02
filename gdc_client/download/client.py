import hashlib
import logging
import os
import re
import sys
import tarfile
import time
import urlparse
from StringIO import StringIO

import requests
from parcel import HTTPClient, UDTClient, utils
from parcel.download_stream import DownloadStream
from progressbar import ETA, Bar, FileTransferSpeed, Percentage, ProgressBar

from gdc_client.defaults import SUPERSEDED_INFO_FILENAME_TEMPLATE
from gdc_client.utils import build_url

log = logging.getLogger('gdc-download')


def fix_url(url):
    # type: (str) -> str
    """ Fix a url to be used in the rest of the program

        example:
            api.gdc.cancer.gov -> https://api.gdc.cancer.gov/
    """
    if not url.endswith('/'):
        url = '{0}/'.format(url)

    if not (url.startswith('https://') or url.startswith('http://')):
        url = 'https://{0}'.format(url)

    return url


class GDCHTTPDownloadClient(HTTPClient):

    annotation_name = 'annotations.txt'

    def __init__(self, uri, download_related_files=True, download_annotations=True,
                 index_client=None, *args, **kwargs):
        """ GDC parcel client that overrides parallel download
        Args:
            uri (str):
            download_related_files (bool):
            download_annotations (bool):
            index_client (gdc_client.query.index.GDCIndexClient): gdc api files index client
        """

        self.base_uri = uri
        self.data_uri = urlparse.urljoin(self.base_uri, 'data/')

        self.annotations = download_annotations
        self.related_files = download_related_files

        self.md5_check = kwargs.get('file_md5sum')

        self.gdc_index_client = index_client
        self.base_directory = kwargs.get('directory')

        super(GDCHTTPDownloadClient, self).__init__(self.data_uri, *args, **kwargs)

    def download_related_files(self, file_id):
        # type: (str) -> None
        """Finds and downloads files related to the primary entity.
        :param str file_id: String containing the id of the primary entity
        """

        # The primary entity's directory
        directory = os.path.join(self.base_directory, file_id)

        related_files = self.gdc_index_client.get_related_files(file_id)
        if related_files:

            log.debug("Found {0} related files for {1}.".format(len(related_files), file_id))
            for related_file in related_files:

                log.debug("related file {0}".format(related_file))
                related_file_url = urlparse.urljoin(self.data_uri, related_file)
                stream = DownloadStream(related_file_url, directory, self.token)

                # TODO: un-set this when parcel is moved to dtt
                # hacky way to get it working like the old dtt
                stream.directory = directory

                # run original parallel download
                super(GDCHTTPDownloadClient, self).parallel_download(stream)

                if os.path.isfile(stream.temp_path):
                    utils.remove_partial_extension(stream.temp_path)
        else:
            log.debug("No related files")

    def download_annotations(self, file_id):
        # type: (str) -> None
        """Finds and downloads annotations related to the primary entity.
        :param str file_id: String containing the id of the primary entity
        """

        # The primary entity's directory
        directory = os.path.join(self.base_directory, file_id)

        annotations = self.gdc_index_client.get_annotations(file_id)

        if annotations:
            log.debug('Found {0} annotations for {1}.'.format(len(annotations), file_id))
            # {'ids': ['id1', 'id2'..., 'idn']}
            ann_ids = {"ids": annotations}

            r = self._post(path='data', json=ann_ids)
            r.raise_for_status()
            tar = tarfile.open(mode="r:gz", fileobj=StringIO(r.content))
            if self.annotation_name in tar.getnames():
                member = tar.getmember(self.annotation_name)
                ann = tar.extractfile(member).read()
                path = os.path.join(directory, self.annotation_name)
                with open(path, 'w') as f:
                    f.write(ann)

                log.debug('Wrote annotations to {0}.'.format(path))

    def _untar_file(self, tarfile_name):
        # type: (str) -> list[str]
        """ untar the file and return all the file names inside the tarfile """

        t = tarfile.open(tarfile_name)
        members = [m for m in t.getmembers() if m.name != 'MANIFEST.txt']
        t.extractall(members=members, path=self.base_directory)
        t.close()

        # cleanup
        os.remove(tarfile_name)

        return [m.name for m in members]

    def _md5_members(self, members):
        # type: (list[str]) -> list[str]
        """ Calculate md5 hash and compare them with values given by the API """

        errors = []
        for m in members:
            if re.findall(SUPERSEDED_INFO_FILENAME_TEMPLATE, m):
                log.warn(
                    'Some of the files have been superseded. See {} '
                    'for reference.'.format(m))
                continue
            member_uuid = m.split('/')[0]
            log.debug('Validating checksum for {0}...'.format(member_uuid))

            md5sum = hashlib.md5()
            filename = os.path.join(self.base_directory, m)
            with open(filename, 'rb') as f:
                md5sum.update(f.read())

            if self.gdc_index_client.get_md5sum(member_uuid) != md5sum.hexdigest():
                log.error('UUID {0} has invalid md5sum'.format(member_uuid))
                errors.append(member_uuid)

        return errors

    def _post(self, path, headers=None, json=None, stream=True):
        # type: (str, dict[str,str], dict[str,object], bool) -> requests.models.Response
        """ custom post request that will query both active and legacy api

        return a python requests object to be handled by the method calling self._post
        """

        r = None
        try:
            # try active
            active = urlparse.urljoin(self.base_uri, path)
            legacy = urlparse.urljoin(self.base_uri, 'legacy/{0}'.format(path))

            r = requests.post(
                active,
                stream=stream,
                verify=self.verify,
                json=json or {},
                headers=headers or {},
            )
            if r.status_code not in [200, 203]:
                # try legacy if active doesn't return OK
                r = requests.post(
                   legacy,
                   stream=stream,
                   verify=self.verify,
                   json=json or {},
                   headers=headers or {},
               )

        except Exception as e:
            log.error(e)

        return r

    def _download_tarfile(self, small_files):
        # type: (list[str]) -> tuple[str, object]
        """ Make the request to the API for the tarfile downloads """

        errors = []
        headers = {
            'X-Auth-Token': self.token,
        }

        # {'ids': ['id1', 'id2'..., 'idn']}
        ids = {"ids": small_files}

        # POST request avoids the MAX LEN character limit for URLs
        params = ('tarfile',)
        path = build_url('data', *params)
        r = self._post(path=path, headers=headers, json=ids)

        if r.status_code == requests.codes.bad:
            log.error('Unable to connect to the API')
            log.error('Is this the correct URL? {0}'.format(self.base_uri))

        elif r.status_code == requests.codes.forbidden:
            # since the files are grouped by access control, that means
            # a group is entirely controlled or open access.
            # If it fails to download because you don't have access then
            # don't bother trying again
            log.error(r.text)
            return '', []

        if r.status_code not in [200, 203]:
            log.warning('[{0}] Unable to download group'.format(r.status_code))
            errors.append(ids['ids'])
            return '', errors

        # {'content-disposition': 'filename=the_actual_filename.tar'}
        content_filename = r.headers.get('content-disposition') or \
            r.headers.get('Content-Disposition')

        if content_filename:
            tarfile_name = os.path.join(
                self.base_directory,
                content_filename.split('=')[1],
            )
        else:
            tarfile_name = time.strftime("gdc-client-%Y%m%d-%H%M%S.tar")

        with open(tarfile_name, 'wb') as f:
            for chunk in r:
                f.write(chunk)

        r.close()

        return tarfile_name, errors

    def download_small_groups(self, smalls):
        # type: (list[str]) -> tuple[list[str], int]
        """ Download small groups

        Smalls are predetermined groupings of smaller file size files.
        They are grouped to reduce the number of open connections per download.
        """

        successful_count = 0
        errors = []
        groupings_len = len(smalls)

        for i, s in enumerate(smalls):
            if len(s) == 0 or s == []:
                log.error('There are no files to download')
                return [], 0

            pbar = ProgressBar(widgets=[
                Percentage(), ' ',
                Bar(marker='#', left='[', right=']'), ' ',
                ETA(), ' ', FileTransferSpeed(), ' '], maxval=1, fd=sys.stdout)
            pbar.start()

            log.debug('Saving grouping {0}/{1}'.format(i+1, groupings_len))
            tarfile_name, error = self._download_tarfile(s)

            # this will happen in the result of an
            # error that shouldn't be retried
            if tarfile_name == '':
                continue

            if error:
                errors += error
                time.sleep(0.5)
                continue

            successful_count += len(s)
            members = self._untar_file(tarfile_name)

            if self.md5_check:
                errors += self._md5_members(members)
            pbar.update(1)
            pbar.finish()

        return errors, successful_count

    def parallel_download(self, stream):

        # gdc-client calls parcel's parallel_download,
        # which is where most of the downloading takes place
        file_id = stream.url.split('/')[-1]
        super(GDCHTTPDownloadClient, self).parallel_download(stream)

        if self.related_files:
            try:
                self.download_related_files(file_id)
            except Exception as e:
                log.warn('Unable to download related files for {0}: {1}'.format(
                    file_id, e))
                if self.debug:
                    raise

        if self.annotations:
            try:
                self.download_annotations(file_id)
            except Exception as e:
                log.warn('Unable to download annotations for {0}: {1}'.format(
                    file_id, e))
                if self.debug:
                    raise


class GDCUDTDownloadClient(GDCHTTPDownloadClient):

    def __init__(self, remote_uri, download_related_files=True,
                 download_annotations=True, *args, **kwargs):

        directory = os.path.abspath(time.strftime("gdc-client-%Y%m%d-%H%M%S"))

        # favoring composition over inheritance
        self.udt_adapter = UDTClient(*args, **kwargs)
        super(GDCUDTDownloadClient, self).__init__(uri=fix_url(remote_uri),
                                                   download_related_files=download_related_files,
                                                   download_annotations=download_annotations,
                                                   directory=directory, *args, **kwargs)

    def construct_local_uri(self, proxy_host, proxy_port, remote_uri):
        return self.udt_adapter.construct_local_uri(proxy_host, proxy_port, remote_uri)

    def start_proxy_server(self, proxy_host, proxy_port, remote_uri):
        return self.udt_adapter.start_proxy_server(proxy_host, proxy_port, remote_uri)
