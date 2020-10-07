from concurrent.futures import ThreadPoolExecutor, as_completed
import contextlib
import copy
import logging
import math
import os
import platform
import random
import time
from collections import deque
from mmap import PAGESIZE, mmap
from multiprocessing import Manager

import requests
import yaml
from lxml import etree
from urllib import parse as urlparse

from gdc_client.parcel.utils import get_file_transfer_pbar, get_percentage_pbar
from gdc_client.upload import manifest

log = logging.getLogger("upload")

MAX_RETRIES = 10
MAX_TIMEOUT = 60
MIN_PARTSIZE = 5242880
DEFAULT_METADATA = ("project_id", "file_name")

OS_WINDOWS = platform.system() == "Windows"

if not OS_WINDOWS:
    from mmap import PROT_READ
else:
    from multiprocessing import freeze_support

    # needed for forking to work
    freeze_support()

    from multiprocessing.pool import ThreadPool as Pool

    # Fake multiprocessing manager namespace
    class FakeNamespace(object):
        def __init__(self):
            self.completed = 0

    from mmap import ALLOCATIONGRANULARITY as PAGESIZE
    from mmap import ACCESS_READ


log = logging.getLogger("upload-client")


class Stream(object):
    def __init__(self, file, pbar, filesize: int):
        self._file = file
        self.pbar = pbar
        self.filesize = filesize

    def __getattr__(self, attr):
        return getattr(self._file, attr)

    def read(self, num):
        chunk = self._file.read(num)

        if self.pbar:
            pbar_value = min(self.pbar.value + len(chunk), self.filesize)
            self.pbar.update(pbar_value)

        return chunk


def upload_multipart(
    filename,
    offset,
    num_bytes,
    url,
    upload_id,
    part_number,
    headers,
    verify=True,
    ns=None,
    debug=False,
):
    tries = MAX_RETRIES
    while tries > 0:
        try:
            log.debug("Start upload part {}".format(part_number))
            with open(filename, "rb") as source:
                if OS_WINDOWS:
                    chunk_file = mmap(
                        fileno=source.fileno(),
                        length=num_bytes,
                        offset=offset,
                        access=ACCESS_READ,
                    )
                else:
                    chunk_file = mmap(
                        fileno=source.fileno(),
                        length=num_bytes,
                        offset=offset,
                        prot=PROT_READ,
                    )

                log.debug("Making http request for part {}".format(part_number))
                res = requests.put(
                    url + "?uploadId={}&partNumber={}".format(upload_id, part_number),
                    headers=headers,
                    data=chunk_file,
                    verify=verify,
                )
                log.debug("Done making http request for part {}".format(part_number))
                chunk_file.close()

            if res.status_code == 200:
                log.debug("Finish upload part {}".format(part_number))

                if ns is not None:
                    ns.completed += 1

                return True

            time.sleep(get_sleep_time(tries))

            tries -= 1
            log.debug("Retry upload part {}, {}".format(part_number, res.content))

        except Exception as e:

            if debug:
                log.exception("Part upload failed: {}. Retrying".format(e))

            time.sleep(get_sleep_time(tries))
            tries -= 1
    return False


def get_sleep_time(tries):
    timeout = min(MAX_TIMEOUT, 2 ** (MAX_RETRIES - tries))
    return timeout * (0.5 + random.random() / 2)


def create_resume_path(file_path):
    """ in case the user enters a path, you want to create
    a resume_filename.yml inside the same directory as the manifest.yml
    """

    # check if it's a path or just a filename
    if os.path.dirname(file_path):
        return "{}/resume_{}".format(
            os.path.dirname(file_path), os.path.basename(file_path)
        )

    # just a filename
    return "resume_" + file_path


class GDCUploadClient(object):
    def __init__(
        self,
        token,
        processes,
        server,
        upload_part_size,
        multipart=True,
        debug=False,
        files=None,
        verify=True,
        manifest_name=None,
    ):
        self.headers = {"X-Auth-Token": token.strip()}
        self.manifest_name = manifest_name
        self.verify = verify
        self.files = files or []
        self.incompleted = deque(copy.deepcopy(self.files))

        if not (server.startswith("http://") or server.startswith("https://")):
            server = "https://" + server

        self.server = server
        self.multipart = multipart
        self.upload_id = None
        self.debug = debug
        self.processes = processes
        self.upload_part_size = (
            int(max(upload_part_size, MIN_PARTSIZE) / PAGESIZE + 1) * PAGESIZE
        )
        self._metadata = {}
        self.resume_path = "resume_{}".format(self.manifest_name)
        self.graphql_url = urlparse.urljoin(self.server, "v0/submission/graphql")

    def _get_node_metadata_via_graphql(
        self, node_id, node_type="node", fields=("project_id", "file_name")
    ):

        query_template = 'query Files { %s (id: "%s") { %s } }'
        query = {
            "query": query_template % (node_type, node_id, " ".join(fields)),
        }

        response = requests.post(
            self.graphql_url, json=query, headers=self.headers, verify=self.verify,
        )
        return response

    def _get_node_type(self, node_id):
        response = self._get_node_metadata_via_graphql(node_id, fields=["type"])

        if response.status_code != 200:
            raise Exception(response.content)

        result = response.json()

        if "errors" in result:
            raise Exception(
                "Fail to query file type: {}".format(", ".join(result["errors"]))
            )

        nodes = result["data"]["node"]

        if not nodes:
            raise Exception("File with id {} not found".format(node_id))

        return nodes[0]["type"]

    def get_metadata(self, node_id, field):
        """
        Get file's project_id and filename from graphql
        """
        if node_id in self._metadata:
            return self._metadata[node_id].get(field)

        file_type = self._get_node_type(node_id)

        fields = (
            DEFAULT_METADATA
            if field in DEFAULT_METADATA
            else DEFAULT_METADATA + (field,)
        )

        # get metadata about file_type
        r = self._get_node_metadata_via_graphql(
            node_id, node_type=file_type, fields=fields,
        )

        if r.status_code != 200:
            raise Exception("Fail to get project_id, filename: {}".format(r.content))

        result = r.json()
        if "errors" in result:
            raise Exception(
                "Fail to query project_id and file_name: {}".format(
                    ", ".join(result["errors"])
                )
            )

        # get first result only
        if len(result["data"][file_type]) > 0:
            self._metadata[node_id] = result["data"][file_type][0]
            return self._metadata[node_id][field]

        raise Exception("File with id {} not found".format(node_id))

    def get_files(self, action="download"):
        """Parse file information from manifest"""
        try:
            self.file_entities = []
            for f in self.files:
                file_entity = FileEntity()
                file_id = f["id"]
                file_entity.node_id = file_id

                project_id = f.get("project_id") or self.get_metadata(
                    file_id, "project_id"
                )
                program, project = [part.upper() for part in project_id.split("-", 1)]

                if not program or not project:
                    raise RuntimeError(
                        "Unable to parse project id {}".format(project_id)
                    )

                file_entity.url = urlparse.urljoin(
                    self.server,
                    "v0/submission/{}/{}/files/{}".format(program, project, file_id),
                )

                if action == "delete":
                    self.file_entities.append(file_entity)
                    continue

                # https://github.com/NCI-GDC/gdcapi/pull/426#issue-146068652
                # [[[ --path takes precedence over everything ]]]
                # -----------------------------------------------
                # 1)--path and f[file_name] from manifest_file
                # 2) --path and UUID's filename, pull filename from API
                # 3) manifest's local_file_path
                # 4) manifest's file_name (in current directory)
                # 5) UUID's resolved files

                # 1) --path and f[file_name] from manifest_file
                if (
                    f.get("path")
                    and f.get("file_name")
                    and os.path.exists(os.path.join(f.get("path"), f.get("file_name")))
                ):
                    file_entity.file_path = os.path.join(
                        f.get("path"), f.get("file_name")
                    )

                # 2) --path and UUID's filename, pull filename from API
                elif (
                    f.get("path")
                    and file_id
                    and os.path.exists(
                        os.path.join(
                            f.get("path"), self.get_metadata(file_id, "file_name")
                        )
                    )
                ):
                    file_entity.file_path = os.path.join(
                        f.get("path"), self.get_metadata(file_id, "file_name")
                    )

                # 3) only local_file_path from manifest file
                elif (
                    f.get("local_file_path")
                    and os.path.basename(f.get("local_file_path"))
                    and os.path.exists(f.get("local_file_path"))
                ):
                    file_entity.file_path = f.get("local_file_path")

                # 4) only file_name provided by manifest
                elif f.get("file_name") and os.path.exists(f.get("file_name")):
                    file_entity.file_path = f.get("file_name")

                # 5) UUID given, get filename from api
                else:
                    file_entity.file_path = self.get_metadata(file_id, "file_name")

                with open(file_entity.file_path, "rb") as fp:
                    file_entity.file_size = os.fstat(fp.fileno()).st_size

                file_entity.upload_id = f.get("upload_id")
                self.file_entities.append(file_entity)

        except KeyError as e:
            log.error("Please provide {} from manifest or as an argument".format(e))
            return False

        # this makes things very hard to debug
        # comment out if you need
        except Exception as e:
            log.error(e)
            return False

    def load_file(self, file_entity):
        # Load attributes from a UploadFile to self for easy access
        self.__dict__.update(file_entity.__dict__)

    def upload(self):
        """ Upload files to the GDC.
        """
        if os.path.isfile(self.resume_path):
            use_resume = input(
                "Found an {}. Press Y to resume last upload and n to start a new upload [Y/n]: ".format(
                    self.resume_path
                )
            )
            if use_resume.lower() not in ["n", "no"]:
                with open(self.resume_path, "r") as f:
                    self.files = manifest.load(f)["files"]

        self.get_files()
        for f in self.file_entities:
            self.load_file(f)

            log.info("Attempting to upload to {}".format(self.url))
            if not self.multipart:
                self._upload()
            else:

                if self.file_size < self.upload_part_size:
                    log.info(
                        "File size smaller than part size {}, do simple upload".format(
                            self.upload_part_size
                        )
                    )
                    self._upload()
                else:
                    self.multipart_upload()
            self.incompleted.popleft()

    def abort(self):
        """ Abort multipart upload"""
        self.get_files()
        for f in self.file_entities:
            self.load_file(f)
            r = requests.delete(
                self.url + "?uploadId={}".format(self.upload_id),
                headers=self.headers,
                verify=self.verify,
            )
            if r.status_code not in [204, 404]:
                raise Exception(
                    "Fail to abort multipart upload: \n{}".format(r.content)
                )
            else:
                log.warning("Abort multipart upload {}".format(self.upload_id))

    def delete(self):
        """Delete file from object storage"""
        self.get_files(action="delete")
        for f in self.file_entities:
            self.load_file(f)
            r = requests.delete(self.url, headers=self.headers, verify=self.verify)
            if r.status_code == 204:
                log.info("Delete file {}".format(self.node_id))
            else:
                log.warning(
                    "Fail to delete file {}: {}".format(self.node_id, r.content)
                )

    def _upload(self):
        """Simple S3 PUT"""

        with open(self.file_path, "rb") as f:
            try:
                r = requests.put(
                    self.url + "/_dry_run", headers=self.headers, verify=self.verify
                )
                if r.status_code != 200:
                    log.error("Can't upload: {}".format(r.content))
                    return

                pbar = get_file_transfer_pbar(self.file_path, self.file_size, desc="Uploading")

                stream = Stream(f, pbar, self.file_size)

                r = requests.put(
                    self.url, data=stream, headers=self.headers, verify=self.verify
                )

                if r.status_code != 200:
                    log.error("Upload failed {}".format(r.content))
                    return

                pbar.finish()

                self.cleanup()
                log.info("Upload finished for file {}".format(self.node_id))
            except Exception as e:
                log.exception(e)
                log.error("Upload failed {}".format(e))

    def multipart_upload(self):
        """S3 Multipart upload"""
        if self.initiate():
            with self.handle_multipart():
                # wait for S3 server to create this multipart upload
                self.check_multipart()
                self.upload_parts()
                self.check_multipart()

                # try again in case some parts failed
                if self.ns.completed != self.total_parts:
                    self.upload_parts()

                if self.debug:
                    log.debug(
                        "Completed: {}/{}".format(self.ns.completed, self.total_parts)
                    )

                self.complete()

    @contextlib.contextmanager
    def handle_multipart(self):
        try:
            yield
            self.upload_id = None
            if os.path.isfile(self.resume_path):
                os.remove(self.resume_path)
        except Exception as e:
            log.warning("Saving unfinished upload file")

            if self.upload_id:
                self.incompleted[0]["upload_id"] = self.upload_id

            path = self.resume_path
            with open(path, "w") as f:
                f.write(
                    yaml.dump(
                        {"files": list(self.incompleted)}, default_flow_style=False
                    )
                )
            log.info("Saved to {}".format(path))

            if self.debug:
                log.exception(e)
                raise

            log.error("Failure: {}".format(e))

    def check_multipart(self):
        tries = MAX_RETRIES

        while tries:
            if self.list_parts() is not None:
                return

            tries -= 1
            time.sleep(get_sleep_time(tries))

        raise Exception(
            "Can't find multipart upload with upload id {}".format(self.upload_id)
        )

    def initiate(self):
        if not self.upload_id:
            r = requests.post(
                self.url + "?uploads", headers=self.headers, verify=self.verify
            )
            if r.status_code == 200:
                xml = XMLResponse(r.content)
                self.upload_id = xml.get_key("UploadId")
                log.info("Start multipart upload. UploadId: {}".format(self.upload_id))
                return True
            else:
                log.error("Fail to initiate multipart upload: {}".format(r.content))
                return False
        return True

    def upload_parts(self):
        args_list = []
        if OS_WINDOWS:
            self.ns = FakeNamespace()
        else:
            manager = Manager()
            self.ns = manager.Namespace()
            self.ns.completed = 0

        part_amount = int(math.ceil(self.file_size / float(self.upload_part_size)))

        self.total_parts = part_amount
        for i in range(part_amount):
            offset = i * self.upload_part_size
            num_bytes = min(self.file_size - offset, self.upload_part_size)
            if not self.multiparts.uploaded(i + 1):
                args_list.append(
                    [
                        self.file_path,
                        offset,
                        num_bytes,
                        self.url,
                        self.upload_id,
                        i + 1,
                        self.headers,
                        self.verify,
                        self.ns,
                        self.debug,
                    ]
                )
            else:
                self.total_parts -= 1

        if self.total_parts == 0:
            return

        pbar = get_percentage_pbar(len(args_list))

        with ThreadPoolExecutor(max_workers=self.processes) as executor:

            future_to_part_number = {
                executor.submit(upload_multipart, *payload): payload[5]
                for payload in args_list
            }

            for future in as_completed(future_to_part_number):
                part_number = future_to_part_number[future]
                log.debug("Part: {} is done".format(part_number))
                pbar.update(self.ns.completed)
        pbar.finish()

    def list_parts(self):
        r = requests.get(
            self.url + "?uploadId={}".format(self.upload_id),
            headers=self.headers,
            verify=self.verify,
        )
        if r.status_code == 200:
            self.multiparts = Multiparts(r.content)
            return self.multiparts
        elif r.status_code in [403, 400]:
            raise Exception(r.content)
        return None

    def complete(self):
        self.check_multipart()
        if self.ns.completed != self.total_parts:
            raise Exception(
                """Multipart upload failed for file {}:
                completed parts: {}, total parts: {}, please try to resume""".format(
                    self.node_id, self.ns.completed, self.total_parts
                )
            )

        url = self.url + "?uploadId={}".format(self.upload_id)
        tries = MAX_RETRIES
        while tries > 0:
            r = requests.post(
                url,
                data=self.multiparts.to_xml(),
                headers=self.headers,
                verify=self.verify,
            )
            if r.status_code != 200:
                tries -= 1
                time.sleep(get_sleep_time(tries))

            else:
                log.info("Multipart upload finished for file {}".format(self.node_id))
                return
        raise Exception("Multipart upload complete failed: {}".format(r.content))

    def cleanup(self):
        if os.path.isfile(self.resume_path):
            os.remove(self.resume_path)


class FileEntity(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

        # be explicit about data members
        self.node_id = None
        self.url = None
        self.file_path = None
        self.path = None
        self.file_size = None
        self.upload_id = None


class Multiparts(object):
    def __init__(self, xml_string):
        self.xml = XMLResponse(xml_string)
        self.parts = self.xml.parse("Part")

    def to_xml(self):
        root = etree.Element("CompleteMultipartUpload")
        for part in self.parts:
            xml_part = etree.SubElement(root, "Part")
            part_number = etree.SubElement(xml_part, "PartNumber")
            part_number.text = part["PartNumber"]
            etag = etree.SubElement(xml_part, "ETag")
            etag.text = part["ETag"]
        return etree.tostring(root, encoding="utf-8")

    def uploaded(self, part_number):
        for part in self.parts:
            if int(part["PartNumber"]) == part_number:
                return True
        return False


class XMLResponse(object):
    def __init__(self, xml_string):
        self.root = etree.fromstring(xml_string)
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
            keys.append({ele.tag.split("}")[-1]: ele.text for ele in element})
        return keys
