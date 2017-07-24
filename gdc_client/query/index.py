import requests
from urlparse import urljoin

import logging
log = logging.getLogger('query')

class GDCIndexClient(object):

    def __init__(self, uri):
        self.uri = uri
        self.metadata = dict()

    def get_related_files(self, file_id):
        # type: str -> List[str]
        return self.metadata[file_id]['related_files']

    def get_annotations(self, file_id):
        # type: str -> List[str]
        return self.metadata[file_id]['annotations']

    def get_md5sum(self, file_id):
        # type: str -> str
        return self.metadata[file_id]['md5sum']

    def get_filesize(self, file_id):
        # type: str -> int
        return int(self.metadata[file_id]['file_size'])

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

        metadata_url = urljoin(self.uri, 'v0/files')

        # using a POST request lets us avoid the MAX URL character length limit
        r = requests.post(metadata_url, json=metadata_query, verify=False)
        json_resp = r.json()
        json_count = json_resp['data']['pagination']['count']

        if r.status_code != requests.codes.ok:

            metadata_url = urljoin(self.uri, 'v0/legacy/files')
            r = requests.post(metadata_url, json=metadata_query, verify=False)
            json_resp = r.json()
            json_count = json_resp['data']['pagination']['count']

            if r.status_code != requests.codes.ok:
                # will be handled by the outermost try block in gdc-client file
                raise Exception('Unable to collect metadata information' \
                        'Is this the correct url? {0}'.format(self.uri))

        hits = r.json()['data']['hits']
        r.close()

        for h in hits:
            related_returns = h.get('index_files', []) + h.get('metadata_files', [])
            related_files = [ r['file_id'] for r in related_returns ]

            annotations = [ a['annotation_id'] for a in h.get('annotations', []) ]

            # set the metadata as a class data member so that it can be
            # references as much as needed without needing to calculate
            # everything over again
            self.metadata[h['id']] = {
                'file_size':     h['file_size'],
                'md5sum':        h['md5sum'],
                'annotations':   annotations,
                'related_files': related_files,
            }

        return self.metadata


    def separate_small_files(self, ids, chunk_size, related_files=False, annotations=False):
        # type: (Set[str], int, bool, bool) -> (List[str], List[List[str]], List[str])
        """Separate the small files from the larger files in
        order to combine them into single downloads. This will reduce
        the number of open connections needed to be made for many small files
        """

        bigs = []
        smalls = []
        potential_smalls = set()

        # go through all the UUIDs and pick out the ones with
        # relate and annotation files so they can be handled by parcel
        self._get_metadata(ids)
        for uuid in self.metadata:

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
                potential_smalls |= set(uuid)

        log.debug('Grouping ids by size')

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

        log.info('{0} total number of files to download'.format(total_count))
        log.info('{0} groupings of files'.format(len(smalls)))

        return bigs, smalls, []
