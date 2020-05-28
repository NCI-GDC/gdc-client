from multiprocessing import cpu_count
import os
from pathlib import Path
import pytest
import tarfile
from typing import List

from gdc_client.parcel.const import HTTP_CHUNK_SIZE, SAVE_INTERVAL
from gdc_client.parcel.download_stream import DownloadStream

from conftest import make_tarfile, md5, uuids
from gdc_client.download.client import GDCHTTPDownloadClient, fix_url
from gdc_client.query.index import GDCIndexClient

BASE_URL = "http://127.0.0.1:5000"


@pytest.mark.usefixtures("setup_mock_server")
class TestDownloadClient:
    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, tmp_path: Path) -> None:
        self.index_client = GDCIndexClient(BASE_URL)
        self.tmp_path = tmp_path
        # use str version to be 3.5 compatible
        self.client_kwargs = self.get_client_kwargs(str(self.tmp_path))
        self.client = self.get_download_client()

    def get_client_kwargs(self, path: str) -> dict:
        return {
            "token": "valid token",
            "n_procs": min(cpu_count(), 8),
            "directory": path,
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

    def get_download_client(self, uuids: List[str] = None) -> GDCHTTPDownloadClient:
        if uuids is not None:
            # get annotation id out of metadata
            self.index_client._get_metadata(uuids)
        return GDCHTTPDownloadClient(
            uri=BASE_URL, index_client=self.index_client, **self.client_kwargs
        )

    def test_download_files_with_fake_uuid_throw_exception_to_developer(self) -> None:
        url_with_fake_uuid = BASE_URL + "/data/fake-uuid"

        with pytest.raises(RuntimeError):
            self.client.download_files([url_with_fake_uuid])

    def test_download_files_with_fake_uuid_not_throw_exception_to_user(self) -> None:
        url_with_fake_uuid = BASE_URL + "/data/fake-uuid"

        self.client_kwargs["debug"] = False
        client_with_debug_off = self.get_download_client()
        client_with_debug_off.download_files([url_with_fake_uuid])

    def test_untar_file(self) -> None:

        files_to_tar = ["small", "small_ann", "small_rel", "small_no_friends"]
        tarfile_name = make_tarfile(files_to_tar)
        self.client._untar_file(tarfile_name)

        assert all((self.tmp_path / f).exists() for f in files_to_tar)

    def test_md5_members(self) -> None:

        files_to_tar = ["small", "small_ann", "small_rel", "small_no_friends"]

        client = self.get_download_client(files_to_tar)

        tarfile_name = make_tarfile(files_to_tar)
        client._untar_file(tarfile_name)
        errors = client._md5_members(files_to_tar)

        assert errors == []

    def test_download_tarfile(self) -> None:
        # this is done after the small file sorting happens,
        # so pick UUIDs that would be grouped together
        files_to_dl = ["small_no_friends"]

        client = self.get_download_client(files_to_dl)

        # it will remove redundant uuids
        tarfile_name, errors = client._download_tarfile(files_to_dl)

        assert tarfile_name is not None
        assert os.path.exists(tarfile_name)
        assert tarfile.is_tarfile(tarfile_name) is True

        with tarfile.open(tarfile_name, "r") as t:
            for member in t.getmembers():
                contents = t.extractfile(member).read().decode()
                assert contents == uuids[member.name]["contents"]

    def test_download_annotations(self) -> None:

        # uuid of file that has an annotation
        small_ann = "small_ann"

        # where we expect annotations to be written
        dir_path = self.tmp_path / small_ann
        dir_path.mkdir()
        file_path = dir_path / "annotations.txt"

        client = self.get_download_client([small_ann])

        # we mock the response from api, a gzipped tarfile with an annotations.txt in it
        # this code will open that and write the annotations.txt to a particular path
        # no return
        client.download_annotations(small_ann)

        # verify
        assert file_path.exists(), "failed to write annotations file"
        assert (
            file_path.read_text() == uuids["annotations.txt"]["contents"]
        ), "annotations content incorrect"

    @pytest.mark.parametrize("check_segments", (True, False))
    def test_no_segment_md5sums_args(self, check_segments: bool) -> None:

        self.client_kwargs["segment_md5sums"] = check_segments
        self.get_download_client()

        assert DownloadStream.check_segment_md5sums is check_segments


def test_fix_url() -> None:
    fixed_url = "https://api.gdc.cancer.gov/"

    assert fix_url("api.gdc.cancer.gov") == fixed_url
    assert fix_url(fixed_url) == fixed_url
    assert fix_url("api.gdc.cancer.gov/") == fixed_url
