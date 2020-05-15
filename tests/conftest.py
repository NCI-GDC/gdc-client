from collections import namedtuple
import hashlib
from io import BytesIO
import pytest
import tarfile

from gdc_client.parcel.const import HTTP_CHUNK_SIZE


def generate_metadata_dict(access, contents, annotations, related_files):
    return {
        "access": access,
        "contents": contents,
        "file_size": None if contents is None else len(contents),
        "md5sum": None if contents is None else md5(contents),
        "annotations": annotations,
        "related_files": related_files,
    }


def md5(iterable):
    md5 = hashlib.md5()

    for chunk in iterable:
        md5.update(chunk.encode("utf-8"))

    return md5.hexdigest()


def make_tarfile(ids, tarfile_name="temp.tar", write_mode="w"):
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


big_content = (str(i) * (HTTP_CHUNK_SIZE + 1) for i in range(1, 5))

uuids = {
    "invalid": generate_metadata_dict(None, None, [], []),
    "small": generate_metadata_dict(
        "controlled", "small content 1", ["annotation 1"], ["related 1"],
    ),
    "small_ann": generate_metadata_dict(
        "open", "small content 2", ["annotations.txt"], [],
    ),
    "small_rel": generate_metadata_dict("open", "small content 3", [], ["related 3"],),
    "small_no_friends": generate_metadata_dict(
        "controlled", "small content 4", [], [],
    ),
    "big": generate_metadata_dict(
        "controlled", next(big_content), ["annotation 1"], ["related 1"],
    ),
    "big_ann": generate_metadata_dict(
        "controlled", next(big_content), ["annotation 2"], [],
    ),
    "big_rel": generate_metadata_dict("open", next(big_content), [], ["related 3"],),
    "big_no_friends": generate_metadata_dict("open", next(big_content), [], [],),
    "annotations.txt": {"contents": "id\tsubmitter_id\t\n123\t456\n"},
}


@pytest.fixture
def versions_response(requests_mock):
    def mock_response(url, ids, latest_ids):
        requests_mock.post(
            url,
            json=[
                {"id": file_id, "latest_id": latest_id}
                for file_id, latest_id in zip(ids, latest_ids)
            ],
        )

    return mock_response
