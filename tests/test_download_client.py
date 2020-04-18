import copy
from multiprocessing import Process, cpu_count
import os
import os.path
import pytest
import tarfile
import tempfile
import time
from unittest import TestCase

from gdc_client.parcel.const import HTTP_CHUNK_SIZE, SAVE_INTERVAL
from gdc_client.parcel.download_stream import DownloadStream

import mock_server
from conftest import make_tarfile, md5, uuids
from gdc_client.download.client import GDCHTTPDownloadClient, fix_url
from gdc_client.query.index import GDCIndexClient

# default values for flask
server_host = "http://127.0.0.1"
server_port = "5000"

# same as --server flag for gdc-client
base_url = server_host + ":" + server_port

client_kwargs = {
    "token": "valid token",
    "n_procs": min(cpu_count(), 8),
    "directory": ".",
    "segment_md5sums": True,
    "file_md5sum": True,
    "debug": True,
    "http_chunk_size": HTTP_CHUNK_SIZE,
    "save_interval": SAVE_INTERVAL,
    "download_related_files": True,
    "download_annotations": True,
    "no_auto_retry": True,
    "retry_amount": 5,
    "verify": True,
}


class DownloadClientTest(TestCase):
    def setUp(self):
        self.server = Process(target=mock_server.app.run)
        self.server.start()

        # give the server time to start
        time.sleep(2)

    def tearDown(self):
        self.server.terminate()
        self.server.join()

    def test_fix_url(self):
        index_client = GDCIndexClient(base_url)
        client = GDCHTTPDownloadClient(
            uri=base_url, index_client=index_client, **client_kwargs
        )

        assert fix_url("api.gdc.cancer.gov") == "https://api.gdc.cancer.gov/"
        assert fix_url("http://api.gdc.cancer.gov/") == "http://api.gdc.cancer.gov/"
        assert fix_url("api.gdc.cancer.gov/") == "https://api.gdc.cancer.gov/"

    def test_untar_file(self):

        files_to_tar = ["small", "small_ann", "small_rel", "small_no_friends"]

        tarfile_name = make_tarfile(files_to_tar)
        index_client = GDCIndexClient(base_url)

        client = GDCHTTPDownloadClient(
            uri=base_url, index_client=index_client, **client_kwargs
        )

        client._untar_file(tarfile_name)

        for f in files_to_tar:
            assert os.path.exists(f)
            os.remove(f)

    def test_md5_members(self):

        files_to_tar = ["small", "small_ann", "small_rel", "small_no_friends"]

        tarfile_name = make_tarfile(files_to_tar)

        index_client = GDCIndexClient(base_url)
        index_client._get_metadata(files_to_tar)

        client = GDCHTTPDownloadClient(
            uri=base_url, index_client=index_client, **client_kwargs
        )

        client._untar_file(tarfile_name)
        errors = client._md5_members(files_to_tar)

        assert errors == []

        for f in files_to_tar:
            os.path.exists(f)
            os.remove(f)

    def test_download_tarfile(self):
        # this is done after the small file sorting happens,
        # so pick UUIDs that would be grouped together
        files_to_dl = ["small_no_friends"]

        index_client = GDCIndexClient(base_url)
        index_client._get_metadata(files_to_dl)

        client = GDCHTTPDownloadClient(
            uri=base_url, index_client=index_client, **client_kwargs
        )

        # it will remove redundant uuids
        tarfile_name, errors = client._download_tarfile(files_to_dl)

        assert tarfile_name != None
        assert os.path.exists(tarfile_name)
        assert tarfile.is_tarfile(tarfile_name) == True

        with tarfile.open(tarfile_name, "r") as t:
            for member in t.getmembers():
                m = t.extractfile(member)
                contents = m.read()
                assert contents.decode("utf-8") == uuids[member.name]["contents"]
                os.remove(tarfile_name)

    def test_download_annotations(self):

        # uuid of file that has an annotation
        files_to_dl = ["small_ann"]

        # get annotation id out of metadata
        index_client = GDCIndexClient(base_url)
        index_client._get_metadata(files_to_dl)

        # set expected output path for download client
        with tempfile.TemporaryDirectory() as tmpdirname:

            override_kwargs = copy.deepcopy(client_kwargs)
            override_kwargs["directory"] = tmpdirname
            # where we expect annotations to be written
            os.mkdir(tmpdirname + "/{}".format(files_to_dl[0]))
            output_path = tmpdirname + "/{}/annotations.txt".format(files_to_dl[0])

            client = GDCHTTPDownloadClient(
                uri=base_url, index_client=index_client, **override_kwargs
            )

            # we mock the response from api, a gzipped tarfile with an annotations.txt in it
            # this code will open that and write the annotations.txt to a particular path
            # no return
            client.download_annotations(files_to_dl[0])

            # verify
            assert os.path.exists(output_path), "failed to write annotations file"
            with open(output_path, "r") as f:
                contents = f.read()
                assert (
                    contents == uuids["annotations.txt"]["contents"]
                ), "annotations content incorrect"


@pytest.mark.parametrize("check_segments", (True, False))
def test_no_segment_md5sums_args(check_segments):

    client_args = copy.deepcopy(client_kwargs)
    client_args["segment_md5sums"] = check_segments
    GDCHTTPDownloadClient(uri=base_url, index_client=None, **client_args)

    assert DownloadStream.check_segment_md5sums is check_segments
