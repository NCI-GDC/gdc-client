from conftest import md5
from gdc_client.query.index import GDCIndexClient
from parcel.const import HTTP_CHUNK_SIZE
from httmock import urlmatch, HTTMock
from unittest import TestCase

base_url = 'https://api.gdc.cancer.gov/'

big = 'big-uuid'
big_contents = ''.join([ '0' for _ in range(HTTP_CHUNK_SIZE + 1) ])
big_file_size = len(big_contents)
big_md5 = md5(big_contents)
big_related = 'big related'
big_annotation = 'big annotation'

small = 'small uuid'
small_contents = 'small content'
small_file_size = len(small_contents)
small_md5 = md5(small_contents)
small_related = 'small related'
small_annotation = 'small annotation'


def _meta_json_success(hits):
    """ boilerplate http request header values """

    return {
        'status_code': 200,
        'headers': {
            'content-type': 'application/json',
        },
        'content': {
            'data': {
                'hits': hits
            },
        },
    }


@urlmatch(netloc=r'api\.gdc\.cancer\.gov', path=r'\/v0\/(legacy\/)?files$')
def full_meta_mock(url, request):
    """ contains file metadata, annotations, and related files """

    return _meta_json_success([{
        'file_size': big_file_size,
        'id': big,
        'md5sum': big_md5,
        'annotations': [{
            'annotation_id': big_annotation,
        }],
        'index_files': [{
            'file_id': big_related,
        }],
    }, {
        'file_size': small_file_size,
        'id': small,
        'md5sum': small_md5,
        'annotations': [{
            'annotation_id': small_annotation,
        }],
        'index_files': [{
            'file_id': small_related,
        }],
    }])


@urlmatch(netloc=r'api\.gdc\.cancer\.gov', path=r'\/v0\/(legacy\/)?files$')
def no_rel_no_ann_meta_mock(url, request):
    """ contains file metadata only """

    return _meta_json_success([{
        'file_size': big_file_size,
        'id': big,
        'md5sum': big_md5,
    }, {
        'file_size': small_file_size,
        'id': small,
        'md5sum': small_md5,
    }])


@urlmatch(netloc=r'api\.gdc\.cancer\.gov', path=r'\/v0\/(legacy\/)?files$')
def rel_meta_mock(url, request):
    """ contains file metadata, and related files """

    return _meta_json_success([{
        'file_size': big_file_size,
        'id': big,
        'md5sum': big_md5,
        'index_files': [{
            'file_id': big_related,
        }],
    }, {
        'file_size': small_file_size,
        'id': small,
        'md5sum': small_md5,
        'index_files': [{
            'file_id': small_related,
        }],
    }])

@urlmatch(netloc=r'api\.gdc\.cancer\.gov', path=r'\/v0\/(legacy\/)?files$')
def ann_meta_mock(url, request):
    """ contains file metadata, and annotations """

    return _meta_json_success([{
        'file_size': big_file_size,
        'id': big,
        'md5sum': big_md5,
        'annotations': [{
            'annotation_id': big_annotation,
        }],
    }, {
        'file_size': small_file_size,
        'id': small,
        'md5sum': small_md5,
        'annotations': [{
            'annotation_id': small_annotation,
        }],
    }])


class QueryIndexTest(TestCase):
    def setup(self):
        pass

    ############ not set ############
    def test_no_metadata_get_related_files(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_related_files(small)
        assert results == []

    def test_no_metadata_get_annotations(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_annotations(small)
        assert results == []

    def test_no_metadata_get_md5sum(self):
        index = GDCIndexClient(uri=base_url)

        results = index.get_md5sum(small)
        assert results == None

    ############ mock metadata ############
    def test_full_mock_get_metadata(self):
        with HTTMock(full_meta_mock):
            index = GDCIndexClient(uri=base_url)
            index._get_metadata([small])

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == [small_related]
        assert index.get_annotations(small) == [small_annotation]

    def test_no_rel_no_ann_mock_get_metadata(self):
        with HTTMock(no_rel_no_ann_meta_mock):
            index = GDCIndexClient(uri=base_url)
            index._get_metadata([small])

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == []
        assert index.get_annotations(small) == []

    def test_ann_mock_get_metadata(self):
        with HTTMock(ann_meta_mock):
            index = GDCIndexClient(uri=base_url)
            index._get_metadata([small])

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == []
        assert index.get_annotations(small) == [small_annotation]

    def test_rel_mock_get_metadata(self):
        with HTTMock(rel_meta_mock):
            index = GDCIndexClient(uri=base_url)
            index._get_metadata([small])

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == [small_related]
        assert index.get_annotations(small) == []


    ############ mock separate small files (smalls) ############
    def test_small_full_separate_small_files(self):
        """ Currently if a file has related or annotation files
        the dtt processes it as if it were a big file so that
        it goes through the old method of downloading,
        regardless of size.

        NOTE: This will probably change in the future.
        """

        with HTTMock(full_meta_mock):
            index = GDCIndexClient(uri=base_url)
            bigs, smalls = index.separate_small_files(
                    [small],
                    HTTP_CHUNK_SIZE,
                    related_files=True,
                    annotations=True)

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == [small_related]
        assert index.get_annotations(small) == [small_annotation]

        assert bigs == [small]
        assert smalls == []

    def test_small_no_rel_no_ann_separate_small_files(self):
        with HTTMock(no_rel_no_ann_meta_mock):
            index = GDCIndexClient(uri=base_url)
            bigs, smalls = index.separate_small_files(
                    [small],
                    HTTP_CHUNK_SIZE,
                    related_files=True,
                    annotations=True)

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == []
        assert index.get_annotations(small) == []

        assert bigs == []
        assert smalls == [[small]]

    def test_small_invalid_separate_small_files(self):
        """ If no metadata can be found about a file, attempt a
        download using the big file method
        """

        invalid = 'invalid uuid'

        with HTTMock(no_rel_no_ann_meta_mock):
            index = GDCIndexClient(uri=base_url)
            bigs, smalls = index.separate_small_files(
                    [invalid],
                    HTTP_CHUNK_SIZE,
                    related_files=True,
                    annotations=True)

        assert index.get_filesize(invalid) == None
        assert index.get_md5sum(invalid) == None
        assert index.get_related_files(invalid) == []
        assert index.get_annotations(invalid) == []

        assert bigs == [invalid]
        assert smalls == []


    ############ mock separate small files (bigs) ############
    def test_big_full_separate_small_files(self):
        with HTTMock(full_meta_mock):
            index = GDCIndexClient(uri=base_url)
            bigs, smalls = index.separate_small_files(
                    [big],
                    HTTP_CHUNK_SIZE,
                    related_files=True,
                    annotations=True)

        assert index.get_filesize(big) == big_file_size
        assert index.get_md5sum(big) == big_md5
        assert index.get_related_files(big) == [big_related]
        assert index.get_annotations(big) == [big_annotation]

        assert bigs == [big]
        assert smalls == []

    ############ mock separate small files (bigs) ############
    def test_big_and_small_full_separate_small_files(self):
        with HTTMock(full_meta_mock):
            index = GDCIndexClient(uri=base_url)
            bigs, smalls = index.separate_small_files(
                    [big, small],
                    HTTP_CHUNK_SIZE,
                    related_files=True,
                    annotations=True)

        assert index.get_filesize(big) == big_file_size
        assert index.get_md5sum(big) == big_md5
        assert index.get_related_files(big) == [big_related]
        assert index.get_annotations(big) == [big_annotation]

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == [small_related]
        assert index.get_annotations(small) == [small_annotation]

        # if a uuid has related files or annotations then they
        # are downloaded as big files
        assert bigs == [big, small]
        assert smalls == []

    def test_big_and_small_no_rel_no_ann_separate_small_files(self):
        with HTTMock(no_rel_no_ann_meta_mock):
            index = GDCIndexClient(uri=base_url)
            bigs, smalls = index.separate_small_files(
                    [big, small],
                    HTTP_CHUNK_SIZE,
                    related_files=True,
                    annotations=True)

        assert index.get_filesize(big) == big_file_size
        assert index.get_md5sum(big) == big_md5
        assert index.get_related_files(big) == []
        assert index.get_annotations(big) == []

        assert index.get_filesize(small) == small_file_size
        assert index.get_md5sum(small) == small_md5
        assert index.get_related_files(small) == []
        assert index.get_annotations(small) == []

        assert bigs == [big]
        assert smalls == [[small]]
