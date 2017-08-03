from parcel.const import HTTP_CHUNK_SIZE
from httmock import urlmatch

import hashlib
import pytest

def md5(iterable):
    md5 = hashlib.md5()
    for chunk in iterable:
        md5.update(chunk)
    return md5.hexdigest()

small_content_1 = 'small content 1'
small_content_2 = 'small content 2'
small_content_3 = 'small content 3'
small_content_4 = 'small content 4'
big_content_1 = ''.join(['1' for _ in xrange(HTTP_CHUNK_SIZE+1) ])
big_content_2 = ''.join(['2' for _ in xrange(HTTP_CHUNK_SIZE+1) ])
big_content_3 = ''.join(['3' for _ in xrange(HTTP_CHUNK_SIZE+1) ])
big_content_4 = ''.join(['4' for _ in xrange(HTTP_CHUNK_SIZE+1) ])

uuids = {
    'small': {
        'contents': small_content_1,
        'file_size': len(small_content_1),
        'md5sum': md5(small_content_1),
        'annotations': ['annotation 1'],
        'related_files': ['related 1'],
    },
    'small_ann': {
        'contents': small_content_2,
        'file_size': len(small_content_2),
        'md5sum': md5(small_content_2),
        'annotations': ['annotation 2'],
    },
    'small_rel': {
        'contents': small_content_3,
        'file_size': len(small_content_3),
        'md5sum': md5(small_content_3),
        'related_files': ['related 3'],
    },
    'small_no_friends': { # :'(
        'contents': small_content_4,
        'file_size': len(small_content_4),
        'md5sum': md5(small_content_4),
    },
    'big': {
        'contents': big_content_1,
        'file_size': len(big_content_1),
        'md5sum': md5(big_content_1),
        'annotations': ['annotation 1'],
        'related_files': ['related 1'],
    },
    'big_ann': {
        'contents': big_content_2,
        'file_size': len(big_content_2),
        'md5sum': md5(big_content_2),
        'annotations': ['annotation 2'],
    },
    'big_rel': {
        'contents': big_content_3,
        'file_size': len(big_content_3),
        'md5sum': md5(big_content_3),
        'related_files': ['related 3'],
    },
    'big_no_friends': { # :'(
        'contents': big_content_4,
        'file_size': len(big_content_4),
        'md5sum': md5(big_content_4),
    },
}


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
def meta_mock(url, request):
    """ contains file metadata, annotations, and related files """

    hits = []
    for uuid, val in uuids.iteritems():
        tmp_dict = {
            'file_size': val['file_size'],
            'id': uuid,
            'md5sum': val['md5sum'],
        }

        if val.get('related_files'):
            tmp_dict['index_files'] = [ {'file_id': r for r in val['related_files']} ]
        if val.get('annotations'):
            tmp_dict['annotations'] = [ {'annotation_id': a for a in val['annotations']} ]

        hits.append(tmp_dict)

    return _meta_json_success(hits)


