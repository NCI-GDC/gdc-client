import requests
from urlparse import urljoin

import logging
log = logging.getLogger('query')

class GDCIndexClient(object):

    def __init__(self, uri):
        self.uri = uri

    def _get_related_files(self, file_id):
        """Query the GDC api for related files.

        :params str file_id: String containing the id of the primary entity
        :returns: A list of related file ids

        """

        r = self._get('files', file_id, fields=['metadata_files.file_id', 'index_files.file_id'])
        related_files = [rf['file_id'] for rf in r['data'].get('metadata_files', [])]
        related_files.extend([rf['file_id'] for rf in r['data'].get('index_files', [])])
        return related_files

    def _get_annotations(self, file_id):
        """Query the GDC api for annotations and download them to a file.

        :params str file_id: String containing the id of the primary entity
        :returns: A list of related file ids

        """

        r = self._get('files', file_id, fields=['annotations.annotation_id'])
        return [a['annotation_id'] for a in r['data'].get('annotations', [])]

    def _get(self, path, ID, fields=[]):
        """GET request to grab metadata from the API

        Used in getting related files and annotations

        """

        url = urljoin(self.uri, 'v0/{}/{}'.format(path, ID))
        params = {'fields': ','.join(fields)} if fields else {}

        r = requests.get(url, verify=False, params=params)
        if r.status_code != requests.codes.ok:
            url = urljoin(self.uri, 'v0/legacy/{}/{}'.format(path, ID))
            r = requests.get(url, verify=False, params=params)
            r.raise_for_status()
        r.close()
        return r.json()


    def separate_small_files(self, ids, chunk_size, related_files=False, annotations=False):
        # type: (Set[string], bool, bool) -> (List[string], List[string] List[List[string]])
        """Separate the small files from the larger files in
        order to combine them into single downloads. This will reduce
        the number of open connections needed to be made for many small files
        """

        bigs = []
        smalls = [[]]

        # deep copy of a set
        given_ids = ids.copy()

        # collect the related/annotation files prior to any downloading
        log.info('Collecting related files')
        extra_files = set()
        for uuid in given_ids:
            # add in the related files
            if related_files:
                log.debug('Collecting related files for {}'.format(uuid))
                try:
                    rf = self._get_related_files(uuid)
                    if rf:
                        extra_files |= set(rf)
                except Exception as e:
                    log.warn('Unable to find related files for {}'.format(uuid))
                    log.error(e)

            # add in the annotations
            if annotations:
                log.debug('Collecting annotation files for {}'.format(uuid))
                try:
                    af = self._get_annotations(uuid)
                    if af:
                        extra_files |= set(af)
                except Exception as e:
                    log.warn('Unable to find annotation files for {}'.format(uuid))
                    log.error(e)

        # now the list of UUIDs contain the related files and annotations
        # and can be grouped into bulk tarfile downloads if applicable
        ids |= extra_files # set union

	filesize_query = {
        'fields': 'file_id,file_size',
        'filters': '{"op":"and","content":['
                   '{"op":"in","content":{'
                   '"field":"files.file_id","value":'
                   '["' + '","'.join(ids) + '"]}}]}',
        'from': '0',
        'size': str(len(ids)), # one (potentially) big ol' request
    }

        filesize_url = urljoin(self.uri, 'v0/files')
        hits = {}

        # using a POST request lets us avoid the MAX URL character length limit
        r = requests.post(filesize_url, json=filesize_query, verify=False)
        if r.status_code == requests.codes.ok:
            hits = r.json()['data']['hits']
            r.close()

        else:
            log.error('Unable to get file sizes. Is this the correct URL? {}'.format(filesize_url))
            # bigs, smalls, errors
            return [], [], list(ids)

        log.debug('Combining IDs into bulk download queries')

        # if the file size is less than chunk_size then group and tarfile it
        # hits = [ {file_id: string, file_size: int} ]
        i = 0
        bundle_size = 0
        for h in hits:
            if bundle_size > chunk_size:
                smalls.append([])
                i += 1
                bundle_size = 0

            if h['file_size'] > chunk_size:
                bigs.append(h['file_id'])
            else:
                smalls[i].append(h['file_id'])
                bundle_size += int(h['file_size'])

        total_files = len(bigs) + sum([ len(s) for s in smalls ])
        if len(given_ids) > total_files:
            log.warning('There are less files to download than originally given')
            log.warning('Number of files originally given: {}'.format(len(given_ids)))

        log.info('{} total number of files to download'.format(total_files))
        log.info('{} groupings of files'.format(len(smalls)))

        # if there are no smalls, then the original list looks like [[]]
        # and we want to just return an empty list
        if smalls[0] == []:
            smalls = []

        return bigs, smalls, []
