from urlparse import urljoin

import logging
import requests


log = logging.getLogger('query')

class GDCIndexClient(object):

    def __init__(self, uri):
        self.uri = uri
        self.metadata = dict()

    def get_related_files(self, uuid):
        # type: str -> List[str]
        if uuid in self.metadata.keys():
            return self.metadata[uuid]['related_files']
        return []

    def get_annotations(self, uuid):
        # type: str -> List[str]
        if uuid in self.metadata.keys():
            return self.metadata[uuid]['annotations']
        return []

    def get_md5sum(self, uuid):
        # type: str -> str
        if uuid in self.metadata.keys():
            return self.metadata[uuid]['md5sum']

    def get_filesize(self, uuid):
        # type: str -> long
        if uuid in self.metadata.keys():
            return long(self.metadata[uuid]['file_size'])

    def _get_metadata(self, uuids):
        # type: List[str] -> Dict[str]str
        """ Capture the metadata of all the UUIDs while making
            as little open connections as possible.

            self.metadata = {
                str file_id: {
                    str       file_size
                    str       md5sum
                    List[str] annotations
                    List[str] related files
                }
            }
        """

        metadata_query = {
            'fields': 'file_id,file_size,md5sum,annotations.annotation_id,' \
                    'metadata_files.file_id,index_files.file_id',
            'filters': '{"op":"and","content":['
                       '{"op":"in","content":{'
                       '"field":"files.file_id","value":'
                       '["' + '","'.join(uuids) + '"]}}]}',
            'from': '0',
            'size': str(len(uuids)), # one big request
        }

        active_meta_url = urljoin(self.uri, 'v0/files')
        legacy_meta_url = urljoin(self.uri, 'v0/legacy/files')

        active_json_resp = dict()
        legacy_json_resp = dict()


        # using a POST request lets us avoid the MAX URL character length limit
        r_active = requests.post(active_meta_url, json=metadata_query, verify=False)
        r_legacy = requests.post(legacy_meta_url, json=metadata_query, verify=False)

        if r_active.status_code == requests.codes.ok:
            active_json_resp = r_active.json()

        if r_legacy.status_code == requests.codes.ok:
            legacy_json_resp = r_legacy.json()

        r_active.close()
        r_legacy.close()

        if not active_json_resp.get('data') and not legacy_json_resp.get('data'):
            log.debug('Unable to retrieve file metadata information. '
                        'continuing downloading as if they were large files')
            return self.metadata

        active_hits = active_json_resp['data']['hits']
        legacy_hits = legacy_json_resp['data']['hits']

        for h in active_hits + legacy_hits:
            related_returns = h.get('index_files', []) + h.get('metadata_files', [])
            related_files = [ r['file_id'] for r in related_returns ]

            annotations = [ a['annotation_id'] for a in h.get('annotations', []) ]

            # set the metadata as a class data member so that it can be
            # references as much as needed without needing to calculate
            # everything over again
            if h['id'] not in self.metadata.keys():
                # don't want to overwrite
                self.metadata[h['id']] = {
                    'file_size':     h['file_size'],
                    'md5sum':        h['md5sum'],
                    'annotations':   annotations,
                    'related_files': related_files,
                }

        return self.metadata


    def separate_small_files(self, ids, chunk_size, related_files=False, annotations=False):
        # type: (Set[str], int, bool, bool) -> (List[str], List[List[str]])
        """Separate the small files from the larger files in
        order to combine them into single downloads. This will reduce
        the number of open connections needed to be made for many small files
        """

        bigs = []
        smalls = []
        potential_smalls = set()

        # go through all the UUIDs and pick out the ones with
        # relate and annotation files so they can be handled by parcel
        log.debug('Grouping ids by size')

        self._get_metadata(ids)
        for uuid in ids:
            if uuid not in self.metadata.keys():
                bigs.append(uuid)
                continue

            rf = self.get_related_files(uuid)
            af = self.get_annotations(uuid)

            # check for related files
            if related_files and rf and uuid not in bigs:
                bigs.append(uuid)

            # check for annotation files
            if annotations and af and uuid not in bigs:
                bigs.append(uuid)

            # if uuid has no related or annotation files
            # then proceed to the small file sorting with them
            if not af and not rf:
                potential_smalls |= set([uuid])

        # the following line is to trigger the first if statement
        # to start the process off properly
        bundle_size = chunk_size + 1

        i = -1

        for uuid in potential_smalls:
            # grouping of file exceeds chunk_size, create a new grouping
            if bundle_size > chunk_size:
                smalls.append([])
                i += 1
                bundle_size = 0

            # individual file is more than chunk_size, big file download
            if self.get_filesize(uuid) > chunk_size:
                bigs.append(uuid)

            # file size is less than chunk_size then group and tarfile it
            else:
                smalls[i].append(uuid)
                bundle_size += self.get_filesize(uuid)

        # for logging/reporting purposes
        total_count = len(bigs) + sum([ len(s) for s in smalls ])
        if len(potential_smalls) > total_count:
            log.warning('There are less files to download than originally given')
            log.warning('Number of files originally given: {0}'.format(len(potential_smalls)))

        log.debug('{0} total number of files to download'.format(total_count))
        log.debug('{0} groupings of files'.format(len(smalls)))

        return bigs, smalls
