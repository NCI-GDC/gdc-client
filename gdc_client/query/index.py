import logging
from json import dumps
from six.moves.urllib.parse import urljoin

import requests

log = logging.getLogger('query')


class GDCIndexClient(object):

    def __init__(self, uri, verify=True):
        self.uri = uri
        self.active_meta_endpoint = '/v0/files'
        self.legacy_meta_endpoint = '/v0/legacy/files'
        self.metadata = dict()
        self.verify = verify

    def get_related_files(self, uuid):
        # type: (str) -> list[str]
        if uuid in self.metadata.keys():
            return self.metadata[uuid]['related_files']
        return []

    def get_annotations(self, uuid):
        # type: (str) -> list[str]
        if uuid in self.metadata.keys():
            return self.metadata[uuid]['annotations']
        return []

    def get_md5sum(self, uuid):
        # type: (str) -> str
        if uuid in self.metadata.keys():
            return self.metadata[uuid]['md5sum']

    def get_filesize(self, uuid):
        # type: (str) -> int
        if uuid in self.metadata.keys():
            return int(self.metadata[uuid]['file_size'])

    def get_access(self, uuid):
        if uuid in self.metadata.keys():
            return self.metadata[uuid]['access']

    def _get_hits(self, url, metadata_query):
        """
        Get hits metadata from a given API endpoint

        Args:
            url (str): Endpoint URL
            metadata_query (dict): Metadata query dictionary

        Returns:
            list: hits from the response data
        """
        json_response = {}
        # using a POST request lets us avoid the MAX URL character length limit
        r = requests.post(url, json=metadata_query, verify=self.verify)

        if r is None:
            return []

        if r.status_code == requests.codes.ok:
            json_response = r.json()

        r.close()

        if (json_response.get('data') is None or
                json_response['data'].get('hits') is None):
            return []

        return json_response['data']['hits']

    def _get_metadata(self, uuids):
        """
        Capture the metadata of all the UUIDs while making as little open
        connections as possible.

        Args:
            uuids (list): A list of UUIDs of the files

        Return:
            dict: metadata information

            self.metadata = {
                str file_id: {
                    str       access
                    str       file_size
                    str       md5sum
                    List[str] annotations
                    List[str] related files
                }
            }
        """

        filters = {
            'op': 'and',
            'content': [{
                'op': 'in',
                'content': {
                    'field': 'files.file_id',
                    # Sometimes a <type 'set'> is passed
                    'value': list(uuids)
                }
            }]
        }

        metadata_query = {
            'fields': 'file_id,file_size,md5sum,annotations.annotation_id,'
                      'metadata_files.file_id,index_files.file_id,access',
            'filters': dumps(filters),
            'from': '0',
            'size': str(len(uuids)),  # one big request
        }

        active_meta_url = urljoin(self.uri, self.active_meta_endpoint)
        legacy_meta_url = urljoin(self.uri, self.legacy_meta_endpoint)

        active_hits = self._get_hits(active_meta_url, metadata_query)
        legacy_hits = self._get_hits(legacy_meta_url, metadata_query)

        if not active_hits and not legacy_hits:
            log.debug('Unable to retrieve file metadata information. '
                      'continuing downloading as if they were large files')
            return self.metadata

        for h in active_hits + legacy_hits:
            related_returns = h.get('index_files', []) + h.get('metadata_files', [])
            related_files = [r['file_id'] for r in related_returns]

            annotations = [a['annotation_id'] for a in h.get('annotations', [])]

            # set the metadata as a class data member so that it can be
            # references as much as needed without needing to calculate
            # everything over again
            if h['id'] not in self.metadata.keys():
                # don't want to overwrite
                self.metadata[h['id']] = {
                    'access':        h['access'],
                    'file_size':     h['file_size'],
                    'md5sum':        h['md5sum'],
                    'annotations':   annotations,
                    'related_files': related_files,
                }

        return self.metadata

    def separate_small_files(self, ids, chunk_size):
        """ Separate big and small files

        Separate the small files from the larger files in
        order to combine them into single grouped downloads. This will reduce
        the number of open connections needed to be made for many small files.

        On top of that, separate the small files by open and controlled access
        so that if a controlled grouping failed, you can handle it as the same
        edge case.

        Args:
            ids (list): a set of file UUIDs
            chunk_size (int): the maximum allowed combined size of small files

        Return:
            list: a list of big file UUIDs
            list: a list of lists of UUIDs. Each inner list representing a
                group of small files
        """

        bigs = set()
        smalls_open = []
        smalls_control = []
        potential_smalls = set()

        # go through all the UUIDs and pick out the ones with
        # relate and annotation files so they can be handled by parcel
        log.debug('Grouping ids by size')

        self._get_metadata(ids)
        for uuid in ids:
            if uuid not in self.metadata.keys():
                bigs.add(uuid)
                continue

            rf = self.get_related_files(uuid)
            af = self.get_annotations(uuid)

            # if there are any related files, add file to a regular/big file
            # download list
            if rf:
                bigs.add(uuid)

            # if there are any annotations, add file to a regular/big file
            # download list
            if af:
                bigs.add(uuid)

            # if uuid has no related or annotation files
            # then proceed to the small file sorting with them
            if not af and not rf:
                potential_smalls.add(uuid)

        # the following line is to trigger the first if statement
        # to start the process off properly
        bundle_open_size = chunk_size + 1
        bundle_control_size = chunk_size + 1

        i_open = -1
        i_control = -1

        for uuid in potential_smalls:
            # grouping of file exceeds chunk_size, create a new grouping
            if bundle_open_size > chunk_size:
                smalls_open.append([])
                i_open += 1
                bundle_open_size = 0

            if bundle_control_size > chunk_size:
                smalls_control.append([])
                i_control += 1
                bundle_control_size = 0

            # individual file is more than chunk_size, big file download
            if self.get_filesize(uuid) > chunk_size:
                bigs.add(uuid)

            # file size is less than chunk_size then group and tarfile it
            else:
                if self.get_access(uuid) == 'open':
                    smalls_open[i_open].append(uuid)
                    bundle_open_size += self.get_filesize(uuid)

                elif self.get_access(uuid) == 'controlled':
                    smalls_control[i_control].append(uuid)
                    bundle_control_size += self.get_filesize(uuid)

        # they are still small files to be downloaded in a group
        smalls = smalls_open + smalls_control

        # for logging/reporting purposes
        total_count = len(bigs) + sum([ len(s) for s in smalls])
        if len(potential_smalls) > total_count:
            log.warning('There are less files to download than originally given')
            log.warning('Number of files originally given: {0}'\
                    .format(len(potential_smalls)))

        log.debug('{0} total number of files to download'.format(total_count))
        log.debug('{0} groupings of files'.format(len(smalls)))

        smalls = [s for s in smalls if s != []]

        return list(bigs), smalls
