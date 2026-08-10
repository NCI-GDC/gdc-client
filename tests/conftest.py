import hashlib
import hmac
import multiprocessing
import tarfile
import time
from collections.abc import Iterable, Mapping
from io import BytesIO
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from gdc_client.parcel import utils
from gdc_client.parcel.const import HTTP_CHUNK_SIZE

_original_hmac_new = hmac.new


def fips_friendly_hmac_new(key, msg=None, digestmod="md5"):
    if digestmod in ("md5", b"md5"):

        def md5_constructor(data=b""):
            return hashlib.md5(data, usedforsecurity=False)

        digestmod = md5_constructor

    return _original_hmac_new(key, msg, digestmod)


patch("hmac.new", side_effect=fips_friendly_hmac_new).start()


def md5(iterable: Iterable):
    md5_fn = utils.md5()

    for chunk in iterable:
        md5_fn.update(chunk.encode("utf-8"))

    return md5_fn.hexdigest()


def make_tarfile(ids: list[str], tarfile_name: str = "temp.tar", write_mode: str = "w") -> str:
    """Make a tarfile for the purposes of testing tarfile methods"""

    # normally small files don't get grouped together if they have
    # related or annotation files, but for this test it's ok

    with tarfile.open(tarfile_name, write_mode) as t:
        for i in ids:
            s = BytesIO()
            s.write(uuids[i]["contents"].encode("utf-8"))

            info = tarfile.TarInfo(name=i)
            info.size = s.tell()

            s.seek(0)
            t.addfile(fileobj=s, tarinfo=info)
            s.close()

    return tarfile_name


def generate_metadata_dict(
    access: str, contents: str, annotations: list[str], related_files: list[str]
) -> Mapping[str, str | list[str]]:
    return {
        "access": access,
        "contents": contents,
        "file_size": None if contents is None else len(contents),
        "md5sum": None if contents is None else md5(contents),
        "annotations": annotations,
        "related_files": related_files,
    }


def get_big_content(n: int) -> str:
    return str(n) * (HTTP_CHUNK_SIZE + 1)


uuids = {
    "invalid": generate_metadata_dict(None, None, [], []),
    "small": generate_metadata_dict(
        "controlled",
        "small content 1",
        ["annotation 1"],
        ["related 1"],
    ),
    "small_ann": generate_metadata_dict(
        "open",
        "small content 2",
        ["annotations.txt"],
        [],
    ),
    "small_rel": generate_metadata_dict(
        "open",
        "small content 3",
        [],
        ["related 3"],
    ),
    "small_no_friends": generate_metadata_dict(
        "controlled",
        "small content 4",
        [],
        [],
    ),
    "big": generate_metadata_dict(
        "controlled",
        get_big_content(1),
        ["annotation 1"],
        ["related 1"],
    ),
    "big_ann": generate_metadata_dict(
        "controlled",
        get_big_content(2),
        ["annotation 2"],
        [],
    ),
    "big_rel": generate_metadata_dict(
        "open",
        get_big_content(3),
        [],
        ["related 3"],
    ),
    "big_no_friends": generate_metadata_dict(
        "open",
        get_big_content(4),
        [],
        [],
    ),
    "annotations.txt": {"contents": "id\tsubmitter_id\t\n123\t456\n"},
}


def run_mock_server():
    # import mock_server here to avoid cyclic import
    import mock_server

    mock_server.app.run()


@pytest.fixture(scope="class")
def setup_mock_server() -> None:
    try:
        ctx = multiprocessing.get_context("fork")
        server = ctx.Process(target=run_mock_server)
    except ValueError:
        server = multiprocessing.Process(target=run_mock_server)

    server.start()
    # originally set to 5 for py38 and macos, up to 10 for macos py310
    time.sleep(10)
    yield
    server.terminate()
    server.join()


@pytest.fixture
def versions_response(requests_mock):
    def mock_response(url: str, ids: list[str], latest_ids: list[str]) -> None:
        requests_mock.post(
            url,
            json=[
                {"id": file_id, "latest_id": latest_id}
                for file_id, latest_id in zip(ids, latest_ids)
            ],
        )

    return mock_response


@pytest.fixture
def versions_response_error(requests_mock):
    def mock_response(url: str) -> None:
        requests_mock.post(
            url,
            content=bytes("<html>502 Bad Gateway</html>", "utf-8"),
            status_code=502,
        )

    return mock_response


@pytest.fixture
def mock_s3_conn():
    with mock_aws():
        conn = boto3.resource("s3")
        yield conn


@pytest.fixture
def mock_s3_bucket(mock_s3_conn):
    bucket = mock_s3_conn.create_bucket(Bucket="test-bucket")
    return bucket
