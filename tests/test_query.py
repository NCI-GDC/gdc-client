import time
from multiprocessing import Process
from unittest import TestCase

import pytest
from gdc_client.parcel.const import HTTP_CHUNK_SIZE

import mock_server
from conftest import uuids
from gdc_client.query.index import GDCIndexClient
from gdc_client.query.versions import _chunk_list, get_latest_versions

# default values for flask
server_host = 'http://127.0.0.1'
server_port = '5000'

# same as --server flag for gdc-client
base_url = server_host + ':' + server_port

class QueryIndexTest(TestCase):
    def setUp(self):
        self.server = Process(target=mock_server.app.run)
        self.server.start()

        # give the server time to start
        time.sleep(0.5)

    def tearDown(self):
        self.server.terminate()

    ############ not set ############
    def test_no_metadata_get_related_files(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_related_files(uuids['small'])
        assert results == []

    def test_no_metadata_get_annotations(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_annotations(uuids['small'])
        assert results == []

    def test_no_metadata_get_md5sum(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_md5sum(uuids['small'])
        assert results == None

    def test_no_metadata_get_filesize(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_filesize(uuids['small'])
        assert results == None

    def test_no_metadata_get_filesize(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_access(uuids['small'])
        assert results == None

    ############ mock metadata ############
    def test_full_mock_get_metadata(self):
        index = GDCIndexClient(uri=base_url)
        index._get_metadata(['small'])

        assert index.get_access('small') == uuids['small']['access']
        assert index.get_filesize('small') == uuids['small']['file_size']
        assert index.get_md5sum('small') == uuids['small']['md5sum']
        assert index.get_related_files('small') == uuids['small']['related_files']
        assert index.get_annotations('small') == uuids['small']['annotations']

    def test_no_rel_no_ann_mock_get_metadata(self):
        index = GDCIndexClient(uri=base_url)
        index._get_metadata(['small_no_friends'])

        assert index.get_access('small_no_friends') == uuids['small_no_friends']['access']
        assert index.get_filesize('small_no_friends') == uuids['small_no_friends']['file_size']
        assert index.get_md5sum('small_no_friends') == uuids['small_no_friends']['md5sum']
        assert index.get_related_files('small_no_friends') == []
        assert index.get_annotations('small_no_friends') == []

    def test_ann_mock_get_metadata(self):
        index = GDCIndexClient(uri=base_url)
        index._get_metadata(['small_ann'])

        assert index.get_access('small_ann') == uuids['small_ann']['access']
        assert index.get_filesize('small_ann') == uuids['small_ann']['file_size']
        assert index.get_md5sum('small_ann') == uuids['small_ann']['md5sum']
        assert index.get_related_files('small_ann') == []
        assert index.get_annotations('small_ann') == uuids['small_ann']['annotations']

    def test_rel_mock_get_metadata(self):
        index = GDCIndexClient(uri=base_url)
        index._get_metadata(['small_rel'])

        assert index.get_access('small_rel') == uuids['small_rel']['access']
        assert index.get_filesize('small_rel') == uuids['small_rel']['file_size']
        assert index.get_md5sum('small_rel') == uuids['small_rel']['md5sum']
        assert index.get_related_files('small_rel') == uuids['small_rel']['related_files']
        assert index.get_annotations('small_rel') == []


    ############ mock separate small files (smalls) ############
    def test_small_full_separate_small_files(self):
        """ Currently if a file has related or annotation files
        the dtt processes it as if it were a big file so that
        it goes through the old method of downloading,
        regardless of size.

        NOTE: This will probably change in the future.
        """

        index = GDCIndexClient(uri=base_url)
        bigs, smalls = index.separate_small_files(
                ['small'],
                HTTP_CHUNK_SIZE)

        assert index.get_access('small') == uuids['small']['access']
        assert index.get_filesize('small') == uuids['small']['file_size']
        assert index.get_md5sum('small') == uuids['small']['md5sum']
        assert index.get_related_files('small') == uuids['small']['related_files']
        assert index.get_annotations('small') == uuids['small']['annotations']

        assert bigs == ['small']
        assert smalls == []

    def test_small_no_rel_no_ann_separate_small_files(self):
        index = GDCIndexClient(uri=base_url)
        bigs, smalls = index.separate_small_files(
                ['small_no_friends'],
                HTTP_CHUNK_SIZE)

        assert index.get_access('small_no_friends') == uuids['small_no_friends']['access']
        assert index.get_filesize('small_no_friends') == uuids['small_no_friends']['file_size']
        assert index.get_md5sum('small_no_friends') == uuids['small_no_friends']['md5sum']
        assert index.get_related_files('small_no_friends') == []
        assert index.get_annotations('small_no_friends') == []

        assert bigs == []
        assert smalls == [['small_no_friends']]

    def test_small_invalid_separate_small_files(self):
        """ If no metadata can be found about a file, attempt a
        download using the big file method
        """

        invalid = 'invalid uuid'

        index = GDCIndexClient(uri=base_url)
        bigs, smalls = index.separate_small_files(
                [invalid],
                HTTP_CHUNK_SIZE)

        assert index.get_access(invalid) == None
        assert index.get_filesize(invalid) == None
        assert index.get_md5sum(invalid) == None
        assert index.get_related_files(invalid) == []
        assert index.get_annotations(invalid) == []

        assert bigs == [invalid]
        assert smalls == []


    ############ mock separate small files (bigs) ############
    def test_big_full_separate_small_files(self):
        index = GDCIndexClient(uri=base_url)
        bigs, smalls = index.separate_small_files(
                ['big'],
                HTTP_CHUNK_SIZE)

        assert index.get_access('big') == uuids['big']['access']
        assert index.get_filesize('big') == uuids['big']['file_size']
        assert index.get_md5sum('big') == uuids['big']['md5sum']
        assert index.get_related_files('big') == uuids['big']['related_files']
        assert index.get_annotations('big') == uuids['big']['annotations']

        assert bigs == ['big']
        assert smalls == []

    ############ mock separate small files (bigs) ############
    def test_big_and_small_full_separate_small_files(self):
        index = GDCIndexClient(uri=base_url)
        bigs, smalls = index.separate_small_files(
                ['big', 'small'],
                HTTP_CHUNK_SIZE)

        assert index.get_access('big') == uuids['big']['access']
        assert index.get_filesize('big') == uuids['big']['file_size']
        assert index.get_md5sum('big') == uuids['big']['md5sum']
        assert index.get_related_files('big') == uuids['big']['related_files']
        assert index.get_annotations('big') == uuids['big']['annotations']

        assert index.get_access('small') == uuids['small']['access']
        assert index.get_filesize('small') == uuids['small']['file_size']
        assert index.get_md5sum('small') == uuids['small']['md5sum']
        assert index.get_related_files('small') == uuids['small']['related_files']
        assert index.get_annotations('small') == uuids['small']['annotations']

        # if a uuid has related files or annotations then they
        # are downloaded as big files
        assert set(bigs) == set(['big', 'small'])
        assert smalls == []

    def test_big_and_small_no_rel_no_ann_separate_small_files(self):
        index = GDCIndexClient(uri=base_url)
        bigs, smalls = index.separate_small_files(
                ['big_no_friends', 'small_no_friends'],
                HTTP_CHUNK_SIZE)

        assert index.get_access('big_no_friends') == uuids['big_no_friends']['access']
        assert index.get_filesize('big_no_friends') == uuids['big_no_friends']['file_size']
        assert index.get_md5sum('big_no_friends') == uuids['big_no_friends']['md5sum']
        assert index.get_related_files('big_no_friends') == []
        assert index.get_annotations('big_no_friends') == []

        assert index.get_access('small_no_friends') == uuids['small_no_friends']['access']
        assert index.get_filesize('small_no_friends') == uuids['small_no_friends']['file_size']
        assert index.get_md5sum('small_no_friends') == uuids['small_no_friends']['md5sum']
        assert index.get_related_files('small_no_friends') == []
        assert index.get_annotations('small_no_friends') == []

        assert bigs == ['big_no_friends']
        assert smalls == [['small_no_friends']]


@pytest.mark.parametrize("case", [
    range(1),
    range(499),
    range(500),
    range(1000),
])
def test_chunk_list(case):
    for chunk in _chunk_list(case):
        assert len(chunk) <= 500


@pytest.mark.parametrize('ids, latest_ids, expected', [
    (['foo', 'bar'], ['foo', 'baz'], {'foo': 'foo', 'bar': 'baz'}),
    (['1', '2', '3'], ['a', 'b', 'c'], {'1': 'a', '2': 'b', '3': 'c'}),
    (['1', '2', '3'], ['a', 'b', None], {'1': 'a', '2': 'b', '3': '3'}),
])
def test_get_latest_versions(versions_response, ids, latest_ids, expected):
    url = 'https://example.com'
    versions_response(url + '/files/versions', ids, latest_ids)

    result = get_latest_versions(url, ids)

    assert result == expected
