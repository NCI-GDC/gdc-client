from urlparse import urljoin
from multiprocessing import Pool, Manager
import requests
from exceptions import KeyError
from lxml import etree
import math
import os
import sys
import yaml
from mmap import mmap, PROT_READ, PAGESIZE
import contextlib
from progressbar import ProgressBar, Percentage, Bar
from collections import deque
import time
import copy
from ..log import get_logger


log = get_logger('download-client')
log.propagate = False


def upload_multipart_wrapper(args):
    return upload_multipart(*args)


class Stream(object):

    def __init__(self, file, pbar, filesize):
        self._file = file
        self.pbar = pbar
        self.filesize = filesize

    def __getattr__(self, attr):
        return getattr(self._file, attr)

    def read(self, num):
        self.pbar.update(min(self.pbar.currval+num, self.filesize))
        return self._file.read(num)


def upload_multipart(filename, offset, bytes, url, upload_id, part_number,
                     headers, verify=True, pbar=None, ns=None):
    tries = 10

    while tries > 0:
        try:
            f = open(filename, 'rb')
            chunk_file = mmap(
                fileno=f.fileno(),
                length=bytes,
                offset=offset,
                prot=PROT_READ
            )
            log.debug("Start upload part {}".format(part_number))
            res = requests.put(
                url +
                "?uploadId={}&partNumber={}".format(upload_id, part_number),
                headers=headers, data=chunk_file, verify=verify)
            chunk_file.close()
            f.close()
            if res.status_code == 200:
                if pbar:
                    pbar.fd = sys.stderr
                    ns.completed += 1
                    pbar.update(ns.completed)
                log.debug("Finish upload part {}".format(part_number))
                return True
            else:
                time.sleep(2)
                tries -= 1
                log.debug("Retry upload part {}".format(part_number))

        except:
            time.sleep(2)
            tries -= 1
    return False


class GDCUploadClient(object):

    def __init__(self, token, processes, server,
                 multipart=True, debug=True, part_size=5242880,
                 files={}, verify=True):
        self.headers = {'X-Auth-Token': token}
        self.verify = verify
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
            if not self.filename:
                raise KeyError("Please provide a file path")
            self.file = open(self.filename, 'rb')
            self.node_id = f['id']
            self.file_size = os.fstat(self.file.fileno()).st_size
            project_id = f['project_id']
            self.upload_id = f.get('upload_id')
            tokens = project_id.split('-')
            program = (tokens[0]).upper()
            project = ('-'.join(tokens[1:])).upper()
            if not program or not project:
                raise RuntimeError('Unable to parse project id {}'
                                   .format(project_id))
            self.url = urljoin(
                self.server, 'v0/submission/{}/{}/files/{}'
                .format(program, project, f['id']))
        except KeyError as e:
            raise KeyError(
                "Please provide {} from manifest or as an argument"
                .format(e.message))

    def called(self, arg):
        if arg:
            self.pbar.update(self.pbar.currval+1)

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
                self.url+"?uploadId={}".format(self.upload_id),
                headers=self.headers, verify=self.verify)
            if r.status_code not in [204, 404]:
                raise Exception(
                    "Fail to abort multipart upload: \n{}".format(r.text))
            else:
                print "Abort multipart upload {}".format(self.upload_id)

    def delete(self):
        '''Delete file from object storage'''
        for f in self.files:
            self.get_file(f)
            r = requests.delete(
                self.url, headers=self.headers, verify=self.verify)
            if r.status_code == 204:
                print "Delete file {}".format(self.node_id)
            else:
                print "Fail to delete file {}: {}".format(self.node_id, r.text)

    def _upload(self):
        '''Simple S3 PUT'''
        self.pbar = ProgressBar(
            widgets=[Percentage(), Bar()], maxval=self.file_size).start()
        with open(self.filename, 'rb') as f:
            try:
                stream = Stream(f, self.pbar, self.file_size)
                r = requests.put(
                    self.url, data=stream, headers=self.headers,
                    verify=self.verify)
                if r.status_code != 200:
                    print "Upload failed {}".format(r.text)
                    return
                self.pbar.finish()
                print "Upload finished for file {}".format(self.node_id)
            except Exception as e:
                print "Upload failed {}".format(e.message)

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
        except Exception as e:
            print "Saving unfinished upload id to file"
            if self.upload_id:
                self.incompleted[0]['upload_id'] = self.upload_id
            path = "resume_manifest_{}".format(time.time())
            with open(path, 'w') as f:
                f.write(
                    yaml.dump({"files": list(self.incompleted)},
                              default_flow_style=False))
            print 'saved to', path
            if self.debug:
                raise
            else:
                print "Failure:", e.message

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
            r = requests.post(
                self.url+"?uploads", headers=self.headers, verify=self.verify)
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
        manager = Manager()
        ns = manager.Namespace()
        ns.completed = 0
        part_amount = int(math.ceil(self.file_size / float(self.part_size)))
        self.pbar = ProgressBar(
            widgets=[Percentage(), Bar()], maxval=part_amount).start()
        try:
            for i in xrange(part_amount):
                offset = i * self.part_size
                remaining_bytes = self.file_size - offset
                bytes = min(remaining_bytes, self.part_size)
                if not self.multiparts.uploaded(i+1):
                    args_list.append([self.filename, offset, bytes,
                                      self.url, self.upload_id, i+1,
                                      self.headers, self.verify,
                                      self.pbar, ns])
            pool.map_async(upload_multipart_wrapper, args_list).get(9999999)
            time.sleep(1)
            pool.close()
            pool.join()
        except KeyboardInterrupt:
            print "Caught KeyboardInterrupt, terminating workers"
            pool.terminate()
            pool.join()
            raise Exception("Process canceled by user")

    def list_parts(self):
        r = requests.get(self.url+"?uploadId={}".format(self.upload_id),
                         headers=self.headers, verify=self.verify)
        if r.status_code == 200:
            self.multiparts = Multiparts(r.text)
            return self.multiparts
        elif r.status_code in [403, 400]:
            raise Exception(r.text)
        return None

    def complete(self):
        self.check_multipart()
        self.pbar.finish()
        url = self.url+"?uploadId={}".format(self.upload_id)
        tries = self.retries
        while tries > 0:
            r = requests.post(url,
                              data=self.multiparts.to_xml(),
                              headers=self.headers,
                              verify=self.verify)
            if r.status_code != 200:
                tries -= 1
                time.sleep(2)
            else:
                print "Multipart upload finished for file {}".format(self.node_id)
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
