from StringIO import StringIO
import hashlib
import os
import requests
import tarfile
import time
import urlparse

from parcel import HTTPClient, UDTClient, utils
from parcel.download_stream import DownloadStream

import logging

log = logging.getLogger('gdc-download')

class GDCDownloadMixin(object):

    annotation_name = 'annotations.txt'

    def download_related_files(self, file_id, directory):
        """Finds and downloads files related to the primary entity.
        :param str file_id: String containing the id of the primary entity
        :param str directory: The primary entity's directory
        """

        related_files = self.index.get_related_files(file_id)
        if related_files:

            log.debug("Found {0} related files for {1}.".format(len(related_files), file_id))
            for related_file in related_files:

                log.debug("related file {0}".format(related_file))
                stream = DownloadStream(
                    related_file, self.data_uri, directory, self.token)
                self._download(self.n_procs, stream)

                if os.path.isfile(stream.temp_path):
                    utils.remove_partial_extension(stream.temp_path)
        else:
            log.debug("No related files")

    def download_annotations(self, file_id, directory):
        """Finds and downloads annotations related to the primary entity.
        :param str file_id: String containing the id of the primary entity
        :param str directory: The primary entity's directory
        """

        annotations = self.index.get_annotations(file_id)
        annotation_list = ','.join(annotations)

        if annotations:
            log.debug('Found {0} annotations for {1}.'.format(
                len(annotations), file_id))
            r = requests.get(
                urlparse.urljoin(self.data_uri, annotation_list),
                params={'compress': True},
                verify=self.verify)
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
        # type: (str) -> List[str]
        """ untar the file and return all the file names inside the tarfile """

        t = tarfile.open(tarfile_name)
        members = [ m for m in t.getmembers() if m.name != 'MANIFEST.txt' ]
        t.extractall(members=members, path=self.directory)
        t.close()

        # cleanup
        os.remove(tarfile_name)

        return members


    def _md5_members(self, members):
        # type: (List[str]) -> List[str]
        """ Calculate md5 hash and compare them with values given by the API """

        errors = []
        for m in members:
            member_uuid = m.name.split('/')[0]

            md5sum = hashlib.md5()
            filename = os.path.join(self.directory, m.name)
            with open(filename, 'rb') as f:
                md5sum.update(f.read())

            if self.index.get_md5sum(member_uuid) != md5sum.hexdigest():
                log.error('UUID {0} has invalid md5sum'.format(member_uuid))
                errors.append(member_uuid)

        return errors


    def _post(self, path, headers={}, json={}, stream=True):
        # type: (str, str, Dict[str]str, Dict[str]str, bool, bool) ->
        #    requests.models.Response
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
               json=json,
               headers=headers,
            )
            if r.status_code != requests.codes.ok:
                # try legacy if active doesn't return OK
                r = requests.post(
                   legacy,
                   stream=stream,
                   verify=self.verify,
                   json=json,
                   headers=headers,
               )

        except Exception as e:
            log.error(e)

        return r


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
        r = self._post(path='data?tarfile', headers=headers, json=ids)

        if not r:
            log.error('Unable to connect to the API')
            log.error('Is this the correct URL? {0}'.format(self.base_uri))
            errors.append(small_files)

        if r.status_code != requests.codes.ok:
            log.warning('[{0}] unable to download group {1} '\
                    .format(r.status_code, i+1))
            errors.append(ids['ids'])

            return '', errors

        # {'content-disposition': 'filename=the_actual_filename.tar'}
        tarfile_name = r.headers.get('content-disposition') or \
                r.headers.get('Content-Disposition')

        if tarfile_name:
            tarfile_name = os.path.join(self.directory, tarfile_name.split('=')[1])
        else:
            tarfile_name = time.strftime("gdc-client-%Y%m%d-%H%M%S.tar")

        with open(tarfile_name, 'wb') as f:
            for chunk in r:
                f.write(chunk)

        r.close()

        return tarfile_name, errors


    def download_small_groups(self, smalls):
        # type: (List[str]) -> List[List[str]], int
        """ Smalls are predetermined groupings of smaller file size files.
        They are grouped to reduce the number of open connections per download

        """

        successful_count = 0
        tarfile_name = None
        errors = []
        groupings_len = len(smalls)
        for i, s in enumerate(smalls):
            if len(s) == 0:
                log.error('There are no files to download')
                return

            log.info('Saving grouping {0}/{1}'.format(i+1, groupings_len))
            tarfile_name, error = self._download_tarfile(s)
            if error:
                errprs += error
                time.sleep(0.5)
                continue

            successful_count += len(s)
            members = self._untar_file(tarfile_name)
            if self.md5_check:
                import pdb; pdb.set_trace()
                errors += self._md5_members(members)

        return errors, successful_count


    def parallel_download(self, stream, download_related_files=None,
                          download_annotations=None, *args, **kwargs):

        # This is a little confusing because gdc-client
        # calls parcel's parallel_download, which is where
        # most of the downloading takes place
        file_id = stream.url.split('/')[-1]
        directory = os.path.join(self.directory, file_id)
        super(GDCDownloadMixin, self).parallel_download(stream)

        if download_related_files or\
           download_related_files is None and self.related_files:
            try:
                self.download_related_files(file_id, directory)
            except Exception as e:
                log.warn('Unable to download related files for {0}: {1}'.format(
                    file_id, e))
                if self.debug:
                    raise

        if download_annotations or\
           download_annotations is None and self.annotations:
            try:
                self.download_annotations(file_id, directory)
            except Exception as e:
                log.warn('Unable to download annotations for {0}: {1}'.format(
                    file_id, e))
                if self.debug:
                    raise

    def fix_url(self, url):
        """ Fix a url to be used in the rest of the program

            example:
                api.gdc.cancer.gov -> https://api.gdc.cancer.gov/
        """
        if not url.endswith('/'):
            url = '{0}/'.format(url)

        if not (url.startswith('https://') or url.startswith('http://')):
            url = 'https://{0}'.format(url)

        return url

class GDCHTTPDownloadClient(GDCDownloadMixin, HTTPClient):

    def __init__(self, uri, index_client, download_related_files=True,
                 download_annotations=True, *args, **kwargs):

        self.base_uri = self.fix_url(uri)
        self.data_uri = urlparse.urljoin(self.base_uri, 'data/')
        self.related_files = download_related_files
        self.annotations = download_annotations
        self.verify = kwargs.get('verify')
        self.md5_check = kwargs.get('file_md5sum')
        self.index = index_client

        self.directory = os.path.abspath(time.strftime("gdc-client-%Y%m%d-%H%M%S"))
        if kwargs.get('directory'):
            self.directory = kwargs.get('directory')

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
