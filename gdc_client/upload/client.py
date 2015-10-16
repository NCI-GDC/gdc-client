from multiprocessing import Pool
import requests
from lxml import etree
import math
import time
from filechunkio import FileChunkIO
import os
from mmap import mmap, PROT_READ, PAGESIZE
import contextlib

def upload_multipart_wrapper(args):
    return upload_multipart(*args)

def upload_multipart(filename, offset, bytes, url,upload_id, part_number, headers):
    tries = 10
    while tries > 0:
        if offset % PAGESIZE == 0:
            # print "chunk size is %s, mmaping chunk" % bytes
            f= open(filename, 'rb')
            chunk_file = mmap(
                fileno=f.fileno(),
                length=bytes,
                offset=offset,
                prot=PROT_READ
            )
            f.close()
        else:
            # print "chunk size is %s, not a multiple of page size, reading with FileChunkIO" % bytes
            chunk_file = FileChunkIO(filename, "r", offset=offset, bytes=bytes)
        print 'upload part {}'.format(part_number)
        res=requests.put(
            url+"?uploadId={}&partNumber={}".format(upload_id, part_number),
            headers=headers, data=chunk_file)
        if res.status_code == 200:
            break
        else:
            print 'try again for part {}'.format(part_number)
            time.sleep(2)
            tries -= 1

class GDCUploadClient(object):
    def __init__(self, url, token, filename, processes, multipart=True, debug=True, part_size=5000000):
        self.url = url
        self.headers = {'X-Auth-Token': token}
        self.filename = filename
        self.file = open(self.filename, 'rb')
        self.multipart = multipart
        self.upload_id = None
        self.debug = debug
        self.aborted = False
        self.processes = processes
        self.part_size = PAGESIZE*2000

    def upload(self):
        if not self.multipart:
            self._upload()
        else:
            self.file_size = os.fstat(self.file.fileno()).st_size
            if  self.file_size < 5000000:
                print "File size smaller than 5M, do simple upload"
                self._upload()
            else:
                self.multipart_upload()

    def multipart_upload(self):
        with self.handle_multipart():
            self.initiate()
            # wait for S3 server to create this multipart upload
            self.check_multipart()
            self.upload_parts()
            self.complete()


    def resume(self, upload_id):
        with self.handle_multipart():
            self.upload_id=upload_id
            self.check_multipart()
            self.upload_parts()
            self.complete()

    def check_multipart(self):
        tries = 10
        while tries:
            if self.list_parts() is None:
                tries -= 1
                time.sleep(1)
            else:
                return
        raise "Can't find multipart upload with upload id {}".format(self.upload_id)

    def initiate(self):
        r = requests.post(self.url+"?uploads", headers=self.headers)
        xml = XMLResponse(r.text)
        self.upload_id = xml.get_key('UploadId')
        print "Start multipart upload: {}".format(self.upload_id)

    @contextlib.contextmanager
    def handle_multipart(self):
        try:
            yield
        except:
            if self.upload_id:
                print "Saving unfinished upload id to file"
                with open("{}_{}".format(self.url.split("/")[-1], self.upload_id),'w') as f:
                    f.write(upload_id)
            if self.debug:
                raise

    def upload_parts(self):
        pool = Pool(processes=self.processes)
        args_list = []
        if self.part_size < 10000000:
            part_amount = self.file_size / self.part_size
        else:
            part_amount = int(math.ceil(self.file_size/ float(self.part_size)))
        for i in xrange(part_amount):
            offset = i * self.part_size
            remaining_bytes = self.file_size - offset
            if remaining_bytes < 2 * self.part_size:
                if remaining_bytes/2 > 5000000:
                    first_part = remaining_bytes/2
                    second_part = remaining_bytes - remaining_bytes/2
                    args_list.append(
                        [self.filename, offset, first_part,
                         self.url, self.upload_id, i+1, self.headers])
                    args_list.append(
                        [self.filename, offset+first_part, second_part,
                         self.url, self.upload_id, i+1, self.headers])
                else:
                    args_list.append(
                        [self.filename, offset, remaining_bytes,
                         self.url, self.upload_id, i+1, self.headers])
            else:
                args_list.append(
                    [self.filename, offset, self.part_size,
                     self.url, self.upload_id, i+1, self.headers])
        pool.map_async(upload_multipart_wrapper, args_list).get(99999999)
        pool.close()
        pool.join()

    def list_parts(self):
        r = requests.get(self.url+"?uploadId={}".format(self.upload_id),
                         headers=self.headers)
        if r.status_code == 200:
            res = XMLResponse(r.text)
            self.all_parts = etree.Element("CompleteMultipartUpload")
            for part in res.parse("Part"):
                xml_part = etree.SubElement(self.all_parts, "Part")
                part_number = etree.SubElement(xml_part, "PartNumber")
                part_number.text = part['PartNumber']
                etag = etree.SubElement(xml_part, "ETag")
                etag.text = part["ETag"]


            return self.all_parts
        return None
        

    def complete(self):
        self.check_multipart()
        body = str(etree.tostring(self.all_parts))
        url = self.url+"?uploadId={}".format(self.upload_id)
        r = requests.post(url,
                data=body, headers=self.headers)
        if r.status_code != 200:
            self.handle_failure()

        else:
            print "Multipart upload finished"

    def abort(self, upload_id=None):
        if upload_id is None:
            upload_id = self.upload_id
        if upload_id is not None and not self.aborted:
            print "Abort multipart upload {}".format(upload_id)
            r = requests.delete(
                self.url+"?uploadId={}".format(upload_id),
                headers=self.headers)
            if r.status_code not in [204, 404]:
                raise Exception("Fail to abort multipart upload: \n{}".format(r.text))
            else:
                self.aborted = True

    def _upload(self):
        print("Attempting to upload to {}".format(self.url))
        r = requests.put(self.url, data=self.file, headers=self.headers)

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
