import requests
from urlparse import urljoin

from ..log import get_logger

# Logging
log = get_logger('query')


class GDCIndexClient(object):

    def __init__(self, uri):
        self.uri = uri if uri.endswith('/') else uri + '/'

    def get_related_files(self, file_id):
        """Query the GDC api for related files.

        :params str file_id: String containing the id of the primary entity
        :returns: A list of related file ids

        """

        r = self.get('files', file_id, fields=['metadata_files.file_id', 'index_files.file_id'])
        related_files = [rf['file_id'] for rf in r['data'].get('metadata_files', [])]
        related_files.extend([rf['file_id'] for rf in r['data'].get('index_files', [])])
        return related_files

    def get_annotations(self, file_id):
        """Query the GDC api for annotations and download them to a file.

        :params str file_id: String containing the id of the primary entity
        :returns: A list of related file ids

        """

        r = self.get('files', file_id, fields=['annotations.annotation_id'])
        return [a['annotation_id'] for a in r['data'].get('annotations', [])]

    def get(self, path, ID, fields=[]):
        url = urljoin(self.uri, 'v0/{path}/{ID}'.format(path=path, ID=ID))
        params = {'fields': ','.join(fields)} if fields else {}
        r = requests.get(url, verify=False, params=params)
        if r.status_code != requests.codes.ok:
            url = urljoin(self.uri, 'v0/legacy/{path}/{ID}'.format(path=path, ID=ID))
            r = requests.get(url, verify=False, params=params)
            r.raise_for_status()
        return r.json()
