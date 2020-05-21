import shutil
from multiprocessing import cpu_count
import os
import pytest
import tarfile
import tempfile

from gdc_client.parcel.const import HTTP_CHUNK_SIZE, SAVE_INTERVAL
from gdc_client.parcel.download_stream import DownloadStream

from conftest import make_tarfile, md5, uuids
from gdc_client.download.client import GDCHTTPDownloadClient, fix_url
from gdc_client.query.index import GDCIndexClient


@pytest.mark.usefixtures("setup_mock_server")
class TestDownloadClient:
    def setup_method(self, method):
        # same as --server flag for gdc-client
        self.base_url = "http://127.0.0.1:5000"
        self.index_client = GDCIndexClient(self.base_url)
        self.tmp_path = tempfile.mkdtemp()
        self.client_kwargs = {
            "token": "valid token",
            "n_procs": min(cpu_count(), 8),
            "directory": self.tmp_path,
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
        self.client = GDCHTTPDownloadClient(
            uri=self.base_url, index_client=self.index_client, **self.client_kwargs
        )

    def teardown_method(self, method):
        shutil.rmtree(self.tmp_path)

    def test_download_files_with_fake_uuid_throw_exception_to_developer(self):
        url_with_fake_uuid = self.base_url + "/data/fake-uuid"

        with pytest.raises(RuntimeError):
            self.client.download_files([url_with_fake_uuid])

    def test_download_files_with_fake_uuid_not_throw_exception_to_user(self):
        url_with_fake_uuid = self.base_url + "/data/fake-uuid"

        self.client_kwargs["debug"] = False
        client_with_debug_off = GDCHTTPDownloadClient(
            uri=self.base_url, index_client=self.index_client, **self.client_kwargs
        )
        client_with_debug_off.download_files([url_with_fake_uuid])

    def test_fix_url(self):
        fixed_url = "https://api.gdc.cancer.gov/"

        assert fix_url("api.gdc.cancer.gov") == fixed_url
        assert fix_url(fixed_url) == fixed_url
        assert fix_url("api.gdc.cancer.gov/") == fixed_url

    def test_untar_file(self):

        files_to_tar = ["small", "small_ann", "small_rel", "small_no_friends"]
        tarfile_name = make_tarfile(files_to_tar)
        self.client._untar_file(tarfile_name)

        for f in files_to_tar:
            assert os.path.exists(os.path.join(self.tmp_path, f))

    def test_md5_members(self):

        files_to_tar = ["small", "small_ann", "small_rel", "small_no_friends"]
        self.index_client._get_metadata(files_to_tar)

        client = GDCHTTPDownloadClient(
            uri=self.base_url, index_client=self.index_client, **self.client_kwargs
        )

        tarfile_name = make_tarfile(files_to_tar)
        client._untar_file(tarfile_name)
        errors = client._md5_members(files_to_tar)

        assert errors == []

    def test_download_tarfile(self):
        # this is done after the small file sorting happens,
        # so pick UUIDs that would be grouped together
        files_to_dl = ["small_no_friends"]

        self.index_client._get_metadata(files_to_dl)

        client = GDCHTTPDownloadClient(
            uri=self.base_url, index_client=self.index_client, **self.client_kwargs
        )

        # it will remove redundant uuids
        tarfile_name, errors = client._download_tarfile(files_to_dl)

        assert tarfile_name is not None
        assert os.path.exists(tarfile_name)
        assert tarfile.is_tarfile(tarfile_name) is True

        with tarfile.open(tarfile_name, "r") as t:
            for member in t.getmembers():
                m = t.extractfile(member)
                contents = m.read()
                assert contents.decode("utf-8") == uuids[member.name]["contents"]

    def test_download_annotations(self):

        # uuid of file that has an annotation
        small_ann = "small_ann"

        # get annotation id out of metadata
        self.index_client._get_metadata([small_ann])

        # where we expect annotations to be written
        dir_path = os.path.join(self.tmp_path, small_ann)
        os.mkdir(dir_path)
        file_path = os.path.join(dir_path, "annotations.txt")

        client = GDCHTTPDownloadClient(
            uri=self.base_url, index_client=self.index_client, **self.client_kwargs
        )

        # we mock the response from api, a gzipped tarfile with an annotations.txt in it
        # this code will open that and write the annotations.txt to a particular path
        # no return
        client.download_annotations(small_ann)

        # verify
        assert os.path.exists(file_path), "failed to write annotations file"
        with open(file_path, "r") as f:
            assert (
                f.read() == uuids["annotations.txt"]["contents"]
            ), "annotations content incorrect"

    @pytest.mark.parametrize("check_segments", (True, False))
    def test_no_segment_md5sums_args(self, check_segments):

        self.client_kwargs["segment_md5sums"] = check_segments
        GDCHTTPDownloadClient(uri=self.base_url, **self.client_kwargs)

        assert DownloadStream.check_segment_md5sums is check_segments
