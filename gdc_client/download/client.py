from StringIO import StringIO
import hashlib
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

    annotation_name = 'annotations.txt'

    def download_related_files(self, index, file_id, directory):
        """Finds and downloads files related to the primary entity.
        :param str file_id: String containing the id of the primary entity
        :param str directory: The primary entity's directory
        """

        related_files = index._get_related_files(file_id)
        if related_files:

            log.debug("Found {} related files for {}.".format(len(related_files), file_id))
            for related_file in related_files:

                log.debug("related file {}".format(related_file))
                stream = DownloadStream(
                    related_file, self.uri, directory, self.token)
                self._download(self.n_procs, stream)

                if os.path.isfile(stream.temp_path):
                    utils.remove_partial_extension(stream.temp_path)
        else:
            log.debug("No related files")

    def download_annotations(self, index, file_id, directory):
        """Finds and downloads annotations related to the primary entity.
        :param str file_id: String containing the id of the primary entity
        :param str directory: The primary entity's directory
        """

        annotations = index._get_annotations(file_id)
        annotation_list = ','.join(annotations)

        if annotations:
            log.debug('Found {} annotations for {}.'.format(
                len(annotations), file_id))
            r = requests.get(
                urlparse.urljoin(self.uri, '/data/{}'.format(annotation_list)),
                params={'compress': True},
                verify=False)
            r.raise_for_status()
            tar = tarfile.open(mode="r:gz", fileobj=StringIO(r.content))
            if self.annotation_name in tar.getnames():
                member = tar.getmember(self.annotation_name)
                ann = tar.extractfile(member).read()
                path = os.path.join(directory, self.annotation_name)
                with open(path, 'w') as f:
                    f.write(ann)

            log.info('Wrote annotations to {}.'.format(path))

    def download_small_groups(self, smalls, md5_dict):
        # type: (List[List[str]], Dict[str]str) -> List[List[str]]
        """Smalls are predetermined groupings of smaller filesize files.
        They are grouped to reduce the number of open connections per download

        """

        headers = {
            'X-Auth-Token': self.token,
        }

        filename = None
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

                # using a POST request lets us avoid
                # the MAX URL character length limit
                # active
                tarfile_url = self.data_uri + '?tarfile'
                r = requests.post(
                        tarfile_url,
                        stream=True,
                        verify=self.verify,
                        json=ids,
                        headers=headers,
                )
                if r.status_code != requests.codes.ok:
                    # legacy
                    tarfile_url = self.data_uri + '/legacy?tarfile'

                    r = requests.post(
                        tarfile_url,
                        stream=True,
                        verify=self.verify,
                        json=ids,
                        headers=headers,
                    )

                    if r.status_code != requests.codes.ok:
                        log.warning('[{0}] unable to download group {1} '\
                                .format(r.status_code, i+1))

                        errors.append(ids['ids'])
                        time.sleep(0.5)
                        continue


                # {'content-disposition': 'filename=the_actual_filename.tar'}
                filename = r.headers.get('content-disposition') or \
                        r.headers.get('Content-Disposition')

                if filename:
                    filename = os.path.join(self.directory, filename.split('=')[1])
                else:
                    filename = time.strftime("gdc-client-%Y%m%d-%H%M%S")
                log.info('Saving grouping {0}/{1}'.format(i+1, groupings_len))
                with open(filename, 'wb') as f:
                    for chunk in r:
                        f.write(chunk)

                r.close()

            except Exception as e:
                log.warning('Grouping download failed: {0}'.format(i+1))
                errors.append(ids['ids'])
                log.warn(e)
                return errors

        if not filename:
            log.error('No tarfile downloaded')
            return errors

        # untar
        t = tarfile.open(filename)
        members = [ m for m in t.getmembers() if m.name != 'MANIFEST.txt' ]
        t.extractall(members=members)
        t.close()

        # check md5sum with what's on the server (provided in md5_dict)
        for m in members:
            member_uuid = m.name.split('/')[0]

            md5sum = hashlib.md5()
            with open(m.name, 'rb') as f:
                md5sum.update(f.read())

            if md5_dict[member_uuid] != md5sum.hexdigest():
                log.error('UUID {0} has invalid md5sum'.format(member_uuid))
                errors.append(member_uuid)

        # cleanup the tarfile at the end
        os.remove(filename)

        return errors


    def parallel_download(self, stream, download_related_files=None,
                          download_annotations=None, *args, **kwargs):

        # This is a little confusing because gdc-client
        # calls parcel's parallel_download, which is where
        # most of the downloading takes place
        file_id = stream.ID.split('/')[-1]
        stream.directory = file_id
        super(GDCDownloadMixin, self).parallel_download(stream)

        index = GDCIndexClient(self.base_uri)


        # Recurse on related files
        if download_related_files or\
           download_related_files is None and self.related_files:
            try:
                self.download_related_files(index, file_id, stream.directory)
            except Exception as e:
                log.warn('Unable to download related files for {}: {}'.format(
                    stream.ID, e))
                if self.debug:
                    raise

        # Recurse on annotations
        if download_annotations or\
           download_annotations is None and self.annotations:
            try:
                self.download_annotations(index, file_id, stream.directory)
            except Exception as e:
                log.warn('Unable to download annotations for {}: {}'.format(
                    stream.ID, e))
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

    def __init__(self, uri, download_related_files=True,
                 download_annotations=True, *args, **kwargs):

        self.base_uri = self.fix_url(uri)
        self.data_uri = urlparse.urljoin(self.base_uri, 'data/')
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
