from multiprocessing import Pool
import requests
from exceptions import KeyError
from lxml import etree
import math
import os
import yaml
from mmap import mmap, PROT_READ, PAGESIZE
import contextlib
from collections import deque
import time
import copy


def upload_multipart_wrapper(args):
    return upload_multipart(*args)


def upload_multipart(filename, offset, bytes, url,
                     upload_id, part_number, headers):
    tries = 10
    while tries > 0:
        f = open(filename, 'rb')
        chunk_file = mmap(
            fileno=f.fileno(),
            length=bytes,
            offset=offset,
            prot=PROT_READ
        )
        f.close()
        print 'upload part {}'.format(part_number)
        res = requests.put(
            url+"?uploadId={}&partNumber={}".format(upload_id, part_number),
            headers=headers, data=chunk_file)
        if res.status_code == 200:
            print "upload part {} succeeds".format(part_number)
            return
        else:
            print 'try again for part {}'.format(part_number)
            time.sleep(2)
            tries -= 1


class GDCUploadClient(object):

    def __init__(self, token, processes, server,
                 multipart=True, debug=True, part_size=5242880,
                 files={}):
        self.headers = {'X-Auth-Token': token}
        self.files = files
        self.incompleted = deque(copy.deepcopy(self.files))
        self.server = server
        self.multipart = multipart
        self.upload_id = None
        self.debug = debug
        self.processes = processes
        self.part_size = (part_size/PAGESIZE+1)*PAGESIZE
        self.retries = 10

    def get_file(self, f):
        '''Parse file information from manifest'''
        try:
            self.filename = f['path']
            self.file = open(self.filename, 'rb')
            self.node_id = f['id']
            self.file_size = os.fstat(self.file.fileno()).st_size
            project_id = f['project_id']
            self.upload_id = f.get('upload_id')
            try:
                tokens = project_id.split('-')
                program = (tokens[0]).upper()
                project = ('-'.join(tokens[1:])).upper()
            except Exception as e:
                raise RuntimeError('Unable to parse project id {}: {}'
                                   .format(project_id), e)
            self.url = self.server + '{}/{}/files/{}'.format(
                program, project, f['id'])
        except KeyError as e:
            raise KeyError(
                "Please provide {} from manifest or as an argument"
                .format(e.message))

    def upload(self):
        '''Upload files to object storage'''
        for f in self.files:
            self.get_file(f)
            print("Attempting to upload to {}".format(self.url))
            if not self.multipart:
                self._upload()
            else:

                if self.file_size < 5242880:
                    print "File size smaller than 5M, do simple upload"
                    self._upload()
                else:
                    self.multipart_upload()
            self.incompleted.popleft()

    def abort(self):
        ''' Abort multipart upload'''
        for f in self.files:
            self.get_file(f)
            r = requests.delete(
                self.url+"?uploadId={}".format(upload_id),
                headers=self.headers)
            if r.status_code not in [204, 404]:
                raise Exception(
                    "Fail to abort multipart upload: \n{}".format(r.text))
            else:
                print "Abort multipart upload {}".format(upload_id)

    def delete(self):
        '''Delete file from object storage'''
        for f in self.files:
            self.get_file(f)
            r = requests.delete(self.url, headers=self.headers)
            if r.status_code == 204:
                print "Delete file {}".format(self.node_id)
            else:
                print "Fail to delete file {}: {}".format(self.node_id, r.text)

    def _upload(self):
        '''Simple S3 PUT'''
        with open(self.filename, 'rb') as f:
            r = requests.put(self.url, data='test', headers=self.headers)
            print r.text, r.status_code

    def multipart_upload(self):
        '''S3 Multipart upload'''
        with self.handle_multipart():
            self.initiate()
            # wait for S3 server to create this multipart upload
            self.check_multipart()
            self.upload_parts()
            self.complete()

    @contextlib.contextmanager
    def handle_multipart(self):
        try:
            yield
            self.upload_id = None
        except:
            print "Saving unfinished upload id to file"
            if self.upload_id:
                self.incompleted[0]['upload_id'] = self.upload_id
            path = "resume_{}".format(time.time())
            with open(path, 'w') as f:
                f.write(
                    yaml.dump({"files": list(self.incompleted)},
                    default_flow_style=False))
            print 'saved to', path
            if self.debug:
                raise

    def check_multipart(self):
        tries = self.retries

        while tries:
            if self.list_parts() is None:
                tries -= 1
                time.sleep(2)
            else:
                return
        raise Exception(
            "Can't find multipart upload with upload id {}"
            .format(self.upload_id))

    def initiate(self):
        if not self.upload_id:
            r = requests.post(self.url+"?uploads", headers=self.headers)
            if r.status_code == 200:
                xml = XMLResponse(r.text)
                self.upload_id = xml.get_key('UploadId')
                print "Start multipart upload: {}".format(self.upload_id)
            else:
                print "Fail to initiate multipart upload: {}".format(r.text)
                raise Exception("Fail to initiate")

    def upload_parts(self):
        pool = Pool(processes=self.processes)
        args_list = []
        part_amount = int(math.ceil(self.file_size / float(self.part_size)))
        for i in xrange(part_amount):
            offset = i * self.part_size
            remaining_bytes = self.file_size - offset
            bytes = min(remaining_bytes, self.part_size)
            if not self.multiparts.uploaded(i+1):
                args_list.append(
                    [self.filename, offset, bytes,
                     self.url, self.upload_id, i+1, self.headers])
        pool.map_async(upload_multipart_wrapper, args_list).get(99999999)
        pool.close()
        pool.join()

    def list_parts(self):
        r = requests.get(self.url+"?uploadId={}".format(self.upload_id),
                         headers=self.headers)
        if r.status_code == 200:
            self.multiparts = Multiparts(r.text)
            return self.multiparts
        elif r.status_code in [403, 400]:
            raise Exception(r.text)
        return None

    def complete(self):
        self.check_multipart()
        url = self.url+"?uploadId={}".format(self.upload_id)
        tries = self.retries
        while tries > 0:
            r = requests.post(url,
                              data=self.multiparts.to_xml(),
                              headers=self.headers)
            if r.status_code != 200:
                tries -= 1
                time.sleep(2)
            else:
                print "Multipart upload finished"
                return
        raise Exception("Multipart upload complete failed: {}".format(r.text))


class Multiparts(object):

    def __init__(self, xml_string):
        self.xml = XMLResponse(xml_string)
        self.parts = self.xml.parse("Part")

    def to_xml(self):
        root = etree.Element("CompleteMultipartUpload")
        for part in self.parts:
            xml_part = etree.SubElement(root, "Part")
            part_number = etree.SubElement(xml_part, "PartNumber")
            part_number.text = part['PartNumber']
            etag = etree.SubElement(xml_part, "ETag")
            etag.text = part["ETag"]
        return str(etree.tostring(root))

    def uploaded(self, part_number):
        for part in self.parts:
            if int(part['PartNumber']) == part_number:
                return True
        return False


class XMLResponse(object):

    def __init__(self, xml_string):
        self.root = etree.fromstring(str(xml_string))
        self.namespace = self.root.nsmap[None]

    def get_key(self, key):
        element = self.root.find("{%s}%s" % (self.namespace, key))
        if element is not None:
            return element.text
        return None

    def parse(self, key):
        elements = self.root.findall("{%s}%s" % (self.namespace, key))
        keys = []
        for element in elements:
            keys.append({ele.tag.split('}')[-1]: ele.text for ele in element})
        return keys
