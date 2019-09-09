from gdc_client.parcel.const import HTTP_CHUNK_SIZE
import sys

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

import hashlib
import tarfile
import pytest


def md5(iterable):
    md5 = hashlib.md5()

    for chunk in iterable:
        if sys.version_info[0] < 3:
            md5.update(chunk)
        else:
            md5.update(chunk.encode('utf-8'))

    return md5.hexdigest()


def make_tarfile(ids, tarfile_name='temp.tar', write_mode='w'):
    """Make a tarfile for the purposes of testing tarfile methods"""

    # normally small files don't get grouped together if they have
    # related or annotation files, but for this test it's ok

    with tarfile.open(tarfile_name, write_mode) as t:
        for i in ids:
            s = StringIO()
            s.write(uuids[i]['contents'])
            s.seek(0)

            info = tarfile.TarInfo(name=i)
            info.size = len(s.buf)

            t.addfile(fileobj=s, tarinfo=info)
            s.close()

    return tarfile_name


small_content_1 = 'small content 1'
small_content_2 = 'small content 2'
small_content_3 = 'small content 3'
small_content_4 = 'small content 4'
big_content_1 = ''.join(['1' for _ in range(HTTP_CHUNK_SIZE+1) ])
big_content_2 = ''.join(['2' for _ in range(HTTP_CHUNK_SIZE+1) ])
big_content_3 = ''.join(['3' for _ in range(HTTP_CHUNK_SIZE+1) ])
big_content_4 = ''.join(['4' for _ in range(HTTP_CHUNK_SIZE+1) ])

uuids = {
    'small': {
        'contents': small_content_1,
        'file_size': len(small_content_1),
        'md5sum': md5(small_content_1),
        'annotations': ['annotation 1'],
        'related_files': ['related 1'],
        'access': 'controlled',
    },
    'small_ann': {
        'contents': small_content_2,
        'file_size': len(small_content_2),
        'md5sum': md5(small_content_2),
        'annotations': ['annotation 2'],
        'access': 'open',
    },
    'small_rel': {
        'contents': small_content_3,
        'file_size': len(small_content_3),
        'md5sum': md5(small_content_3),
        'related_files': ['related 3'],
        'access': 'open',
    },
    'small_no_friends': { # :'(
        'contents': small_content_4,
        'file_size': len(small_content_4),
        'md5sum': md5(small_content_4),
        'access': 'controlled',
    },
    'big': {
        'contents': big_content_1,
        'file_size': len(big_content_1),
        'md5sum': md5(big_content_1),
        'annotations': ['annotation 1'],
        'related_files': ['related 1'],
        'access': 'controlled',
    },
    'big_ann': {
        'contents': big_content_2,
        'file_size': len(big_content_2),
        'md5sum': md5(big_content_2),
        'annotations': ['annotation 2'],
        'access': 'controlled',
    },
    'big_rel': {
        'contents': big_content_3,
        'file_size': len(big_content_3),
        'md5sum': md5(big_content_3),
        'related_files': ['related 3'],
        'access': 'open',
    },
    'big_no_friends': { # :'(
        'contents': big_content_4,
        'file_size': len(big_content_4),
        'md5sum': md5(big_content_4),
        'access': 'open',
    },
}


@pytest.fixture
def versions_response(requests_mock):
    def mock_response(url, ids, latest_ids):
        requests_mock.post(url, json=[
            {'id': file_id, 'latest_id': latest_id}
            for file_id, latest_id in zip(ids, latest_ids)
        ])

    return mock_response
