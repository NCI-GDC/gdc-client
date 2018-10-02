from flask import Flask, Response, jsonify, request
from io import StringIO
from conftest import uuids, make_tarfile

import json
import os
import tarfile

app = Flask(__name__)

@app.route('/v0/files', methods=['POST'])
@app.route('/files', methods=['POST'])
@app.route('/legacy/files', methods=['POST'])
@app.route('/v0/legacy/files', methods=['POST'])
def files():
    result = {
        "data": {
            "hits": [],
            "pagination": {
                "count": 0,
                "sort":  "",
                "from":  0,
                "page":  0,
                "total": 0,
                "pages": 0,
                "size":  0
            }
        },
        "warnings": {}
    }

    '''
    [{
        'access':      'access level',
        'md5sum':      'md5.,
        'file_size':   1,
        'id':          'id.,
        'file_id':     'id.
        'annotations': [{'annotation_id': 'id'}]
        'index_files': [{'file_id': 'id'}]
    }]
    '''

    args = request.json
    if not args:
        return ''

    try:
        filters = args.get('filters')
        fields  = args.get('fields')
        size    = args.get('size')

        if not size.isdigit():
            return jsonify(result)

        field_uuids = json.loads(filters)['content'][0]['content']['value']

        for s in range(int(size)):
            if not uuids:
                continue

            uuid = field_uuids.pop()
            node = uuids.get(uuid)

            hit = {}

            if not node:
                continue

            if 'file_id' in fields:
                hit['id'] = uuid

            if 'access' in fields and node.get('access'):
                hit['access'] = node['access']

            if 'file_size' in fields and node.get('file_size'):
                hit['file_size'] = node['file_size']

            if 'md5sum' in fields and node.get('md5sum'):
                hit['md5sum'] = node['md5sum']

            if 'annotations' in fields and node.get('annotations'):
                hit['annotations'] = \
                        [{'annotation_id': a} for a in node['annotations']]

            if 'index_files' in fields and node.get('related_files'):
                hit['index_files'] = \
                        [{'file_id': r} for r in node['related_files']]

            result['data']['hits'].append(hit)

    except Exception as e:
        print('Error', e)

    result['data']['pagination']['size'] = size
    return jsonify(result)

@app.route('/data', methods=['POST'])
@app.route('/v0/data', methods=['POST'])
@app.route('/legacy/data', methods=['POST'])
@app.route('/v0/legacy/data', methods=['POST'])
@app.route('/data/<ids>', methods=['GET'])
@app.route('/v0/data/<ids>', methods=['GET'])
@app.route('/legacy/data/<ids>', methods=['GET'])
@app.route('/v0/legacy/data/<ids>', methods=['GET'])
def download(ids=''):

    data = ''
    filename = 'test_file.txt'

    ids = ids.split(',')

    args = request.json
    if args:
        ids = args.get('ids')

    if type(ids) in [str, str]:
        ids = [ids]

    for i in ids:
        if i not in list(uuids.keys()):
            return '{0} does not exist in {1}'.format(i, list(uuids.keys()))

    is_tarfile  = request.args.get('tarfile') is not None
    is_compress = request.args.get('compress') is not None or len(ids) > 1

    if is_tarfile:
        filename = 'test_file.tar'
        write_mode = 'w|'

    if is_compress:
        write_mode = 'w|gz'
        filename = 'test_file.tar.gz'

    if is_tarfile or is_compress:
        # make tarfile
        make_tarfile(ids, filename)

        # load tarfile into memory to be returned
        with open(filename, 'rb') as f:
            data = f.read()

        # delete tarfile so it can be downloaded by client
        os.remove(filename)

    else:
        data = uuids[ids[0]]['contents']

    resp = Response(data)
    resp.headers['Content-Disposition'] = \
        'attachment; filename={0}'.format(filename)

    resp.headers['Content-Type'] = 'application/octet-stream'
    return resp

