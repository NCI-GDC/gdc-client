"""gdc_client.download.client

Most of the download functionality is in this module.
"""

import hashlib
import logging
import os
import sys
import tarfile
import time
import urlparse
from StringIO import StringIO

from parcel import (
    HTTPClient,
    # UDTClient,
    utils,
)
from parcel.download_stream import DownloadStream
from progressbar import Bar, ETA, FileTransferSpeed, Percentage, ProgressBar
import requests


class GDCDownloadMixin(object):
    """Mixin class used by http and udp client classes"""

    def __init__(self, **kwargs):
        """Init method for download classes"""

        self.logger = logging.getLogger('gdc-download')
        self.annotation_name = 'annotations.txt'

        # unpack kwargs
        self.annotations = kwargs.get('download_annotations', True)
        self.base_directory = kwargs.get('directory')
        self.base_uri = fix_url(kwargs.get('uri'))
        self.data_uri = urlparse.urljoin(self.base_uri, 'data/')
        self.index = kwargs.get('index_client')
        self.md5_check = kwargs.get('file_md5sum')
        self.related_files = kwargs.get('download_related_files', True)
        self.token = kwargs.get('token')
        self.verify = kwargs.get('verify', True)

    def download_related_files(self, file_id):
        """Finds and downloads files related to the primary entity.

        Args:
            file_id (str): String containing the id of the primary entity
        """

        # The primary entity's directory
        directory = os.path.join(self.base_directory, file_id)

        related_files = self.index.get_related_files(file_id)
        if related_files:

            self.logger.debug("Found %d related files for %s.",
                              len(related_files), file_id)
            for related_file in related_files:

                self.logger.debug("related file %s", related_file)
                related_file_url = urlparse.urljoin(self.data_uri, related_file)
                stream = DownloadStream(related_file_url, directory, self.token)

                # TODO: un-set this when parcel is moved to dtt
                # hacky way to get it working like the old dtt
                stream.directory = directory

                self._download(self.n_procs, stream)

                if os.path.isfile(stream.temp_path):
                    utils.remove_partial_extension(stream.temp_path)
        else:
            self.logger.debug("No related files")

    def download_annotations(self, file_id):
        """Finds and downloads annotations related to the primary entity.

        Args:
            file_id (str): String containing the id of the primary entity
        """

        # The primary entity's directory
        directory = os.path.join(self.base_directory, file_id)

        annotations = self.index.get_annotations(file_id)
        annotation_list = ','.join(annotations)

        if annotations:
            self.logger.debug('Found %d annotations for %s.',
                              len(annotations), file_id)
            resp = requests.get(
                urlparse.urljoin(self.data_uri, annotation_list),
                params={'compress': True},
                verify=self.verify)
            resp.raise_for_status()
            tar = tarfile.open(mode="r:gz", fileobj=StringIO(resp.content))
            if self.annotation_name in tar.getnames():
                member = tar.getmember(self.annotation_name)
                ann = tar.extractfile(member).read()
                path = os.path.join(directory, self.annotation_name)
                with open(path, 'w') as annotation_file:
                    annotation_file.write(ann)

            self.logger.debug('Wrote annotations to %s.', path)


    def _untar_file(self, tarfile_name):
        """Untar the file and return all the file names inside the tarfile

        Args:
            tarfile_name (str): save tarfile as this name

        Returns:
            list: tarfile member's file names
        """

        tar_file = tarfile.open(tarfile_name)
        members = [m for m in tar_file.getmembers() if m.name != 'MANIFEST.txt']
        tar_file.extractall(members=members, path=self.base_directory)
        tar_file.close()

        # cleanup
        os.remove(tarfile_name)

        return [m.name for m in members]


    def _md5_members(self, members):
        # type: (List[str]) -> List[str]
        """Calculate md5 hash and compare them with values given by the API

        """

        errors = []
        for member in members:
            member_uuid = member.split('/')[0]
            self.logger.debug('Validating checksum for %s...', member_uuid)

            md5sum = hashlib.md5()
            filename = os.path.join(self.base_directory, member)
            with open(filename, 'rb') as member_file:
                md5sum.update(member_file.read())

            if self.index.get_md5sum(member_uuid) != md5sum.hexdigest():
                self.logger.error('UUID %s has invalid md5sum', member_uuid)
                errors.append(member_uuid)

        return errors


    def _post(self, path, headers=None, json=None, stream=True):
        # type: (str, Dict[str]str, Dict[str]str, bool) -> requests.models.Response
        """Custom post request that will query both active and legacy api

        return a python requests object to be handled by the method calling self._post
        """

        resp = None
        try:
            # try active
            active = urlparse.urljoin(self.base_uri, path)
            legacy = urlparse.urljoin(self.base_uri, 'legacy/{0}'.format(path))

            resp = requests.post(
                active,
                stream=stream,
                verify=self.verify,
                json=json,
                headers=headers,
            )
            if resp.status_code != 200:
                # try legacy if active doesn't return OK
                resp = requests.post(
                    legacy,
                    stream=stream,
                    verify=self.verify,
                    json=json,
                    headers=headers,
                )

        except Exception as error:
            self.logger.error(error)

        return resp


    def _download_tarfile(self, small_files):
        # type: (List[str]) -> str, List[str]
        """ Make the request to the API for the tarfile downloads """

        tarfile_name = ""
        errors = []
        headers = {
            'X-Auth-Token': self.token,
        }

        # {'ids': ['id1', 'id2'..., 'idn']}
        ids = {"ids": small_files}

        # POST request avoids the MAX LEN character limit for URLs
        resp = self._post(path='data?tarfile', headers=headers, json=ids)

        if resp.status_code == 400:
            self.logger.error('Unable to connect to the API')
            self.logger.error('Is this the correct URL? %s', self.base_uri)

        elif resp.status_code == 403:
            # since the files are grouped by access control, that means
            # a group is entirely controlled or open access.
            # If it fails to download because you don't have access then
            # don't bother trying again
            self.logger.error(resp.text)
            return '', []

        if resp.status_code != 200:
            self.logger.warning('[%d] Unable to download group', resp.status_code)
            errors.append(ids['ids'])
            return '', errors

        # {'content-disposition': 'filename=the_actual_filename.tar'}
        content_filename = resp.headers.get('content-disposition') or \
                resp.headers.get('Content-Disposition')

        if content_filename:
            tarfile_name = os.path.join(
                self.base_directory,
                content_filename.split('=')[1])
        else:
            tarfile_name = time.strftime("gdc-client-%Y%m%d-%H%M%S.tar")

        with open(tarfile_name, 'wb') as tar_file:
            for chunk in resp:
                tar_file.write(chunk)

        resp.close()

        return tarfile_name, errors


    def download_small_groups(self, smalls):
        # type: (List[str]) -> List[str], int
        """ Download small groups

        Smalls are predetermined groupings of smaller file size files.
        They are grouped to reduce the number of open connections per download.
        """


        successful_count = 0
        tarfile_name = None
        errors = []
        groupings_len = len(smalls)

        for i, small in enumerate(smalls):
            if not small:
                self.logger.error('There are no files to download')
                return [], 0

            pbar = ProgressBar(widgets=[
                Percentage(), ' ',
                Bar(marker='#', left='[', right=']'), ' ',
                ETA(), ' ', FileTransferSpeed(), ' '], maxval=1, fd=sys.stdout)
            pbar.start()

            self.logger.debug('Saving grouping %d/%d', i+1, groupings_len)
            tarfile_name, error = self._download_tarfile(small)

            # this will happen in the result of an
            # error that shouldn't be retried
            if tarfile_name == '':
                continue

            if error:
                errors += error
                time.sleep(0.5)
                continue

            successful_count += len(small)
            members = self._untar_file(tarfile_name)

            if self.md5_check:
                errors += self._md5_members(members)
            pbar.update(1)
            pbar.finish()

        return errors, successful_count


    def parallel_download(self, stream, download_related_files=None,
                          download_annotations=None):
        """Call parcel's http parallel_download function then post-process.

        Download related files and annotations separately and combine them
        at the end.

        Args:
            stream (parcel.download_stream.DownloadStream): parcel download object
            download_related_files (bool):
            download_annotations (bool):

        Returns:
            <fill-me-in>
        """

        # gdc-client calls parcel's parallel_download,
        # which is where most of the downloading takes place
        file_id = stream.url.split('/')[-1]
        super(GDCDownloadMixin, self).parallel_download(stream)

        if download_related_files or \
           download_related_files is None and self.related_files:
            try:
                self.download_related_files(file_id)
            except Exception as error:
                self.logger.warn('Unable to download related files for %s: %s',
                                 file_id, error)
                if self.debug:
                    raise

        if download_annotations or \
           download_annotations is None and self.annotations:
            try:
                self.download_annotations(file_id)
            except Exception as error:
                self.logger.warn('Unable to download annotations for %s: %s',
                                 file_id, error)
                if self.debug:
                    raise

class GDCHTTPDownloadClient(GDCDownloadMixin, HTTPClient):
    """HTTP download"""
    pass

# class GDCUDTDownloadClient(GDCDownloadMixin, UDTClient):
#     """UDP download"""
#     pass

def fix_url(url):
    """Fix a url to be used in the rest of the program

    Args:
        url (str): potentially unformatted url
    Returns:
        str: fixed url

    Example:
        api.gdc.cancer.gov -> https://api.gdc.cancer.gov/
    """
    if not url.endswith('/'):
        url = '{0}/'.format(url)

    if not (url.startswith('https://') or url.startswith('http://')):
        url = 'https://{0}'.format(url)

    return url
