import boto3
import httmock
import json
import os
import pytest
import re

from collections import namedtuple
from gdc_client.upload import client
from lxml import etree
from typing import Optional
from urllib.parse import parse_qs
from xml.etree.ElementTree import Element
from xmltodict import parse


QueryParts = namedtuple("QueryParts", field_names=["node_type", "node_id", "fields"])
FIVE_MB = 5 * 1024 * 1024


def parse_graphql_query(query: str) -> Optional[QueryParts]:
    # This is based entirely on the known GraphQL query that is sent to backend
    graphql_re = (
        r"^query \w+ \{ "
        r"(?P<node_type>\w+)"
        r" \(id: \""
        r"(?P<node_id>[a-z0-9-]+)"
        r"\"\) \{ "
        r"(?P<fields>[\w ]+)"
        r" } }$"
    )
    parts = re.match(graphql_re, query)

    if not parts:
        return None

    return QueryParts(
        parts.group("node_type"), parts.group("node_id"), parts.group("fields"),
    )


@pytest.fixture
def mock_simple_upload_client(mock_dir_path):
    return client.GDCUploadClient(
        token="dummy",
        processes=2,
        server="localhost",
        upload_part_size=FIVE_MB,
        multipart=False,
        files=[
            {"id": "file-id-2", "path": mock_dir_path},
            {"id": "file-id-1", "project_id": "GDC-MISC", "path": mock_dir_path},
        ],
        debug=False,
    )


@pytest.fixture
def mock_mp_upload_client(mock_dir_path):
    return client.GDCUploadClient(
        token="dummy",
        processes=2,
        server="localhost",
        upload_part_size=FIVE_MB,
        multipart=True,
        files=[
            {"id": "file-id-2", "path": mock_dir_path},
            {"id": "file-id-1", "project_id": "GDC-MISC", "path": mock_dir_path},
        ],
        debug=False,
    )


@pytest.fixture
def mock_dir(tmpdir):
    """Create directory for upload files"""
    bam_dir = tmpdir.mkdir("bams")
    return bam_dir


@pytest.fixture
def mock_dir_path(mock_dir):
    return str(mock_dir)


@pytest.fixture
def mock_files(mock_dir):
    """Create 2 files: 10MB and 15MB in size"""
    file_name1 = "fake_bam_1.bam"
    file_name2 = "fake_bam_2.bam"

    # Create 10MB file
    f1 = mock_dir.join(file_name1)
    size1 = 10 * 1024 * 1024
    f1.write("a" * size1)

    # Create 15MB file
    f2 = mock_dir.join(file_name2)
    size2 = 15 * 1024 * 1024
    f2.write("b" * size2)

    return file_name1, file_name2


@pytest.fixture
def mock_graphql_responses(mock_files):
    file_id1 = "file-id-1"
    file_id2 = "file-id-2"

    metadata = {
        file_id1: {
            "type": "submitted_unaligned_reads",
            "project_id": "GDC-MISC",
            "file_name": mock_files[0],
        },
        file_id2: {
            "type": "submitted_unaligned_reads",
            "project_id": "GDC-MISC",
            "file_name": mock_files[1],
        },
    }

    @httmock.urlmatch(netloc="localhost", method="POST", path="/v0/submission/graphql")
    def handle_graphql(_, req):
        body = json.loads(req.body.decode())
        query = body["query"]

        parsed = parse_graphql_query(query)

        if not parsed:
            return httmock.response(status_code=400, content="Invalid request")

        node_type = parsed.node_type
        file_id = parsed.node_id

        data = {"data": {node_type: []}}

        if file_id not in metadata:
            return httmock.response(200, json.dumps(data))

        data["data"][node_type].append(metadata[file_id])

        return httmock.response(200, json.dumps(data))

    return handle_graphql


def make_xml_tree(tag, value):
    """Create an XML for the given `tag` and `value`

    Create leaf elements only from primitive types and special case Element (this
    is done due to the fact that StorageClasses contain a list of Element's),
    recursively convert nested dict and list objects
    """
    if isinstance(value, str):
        elem = etree.Element(tag)
        elem.text = value
        return elem

    # NOTE: This is a hack, since for some reason `StorageClasses` has a list
    #   of Element objects, instead of dictionaries/primitives like everything else
    if isinstance(value, Element):
        elem = etree.Element(tag)
        elem.text = value.text
        return elem

    if isinstance(value, dict):
        element = etree.Element(tag)
        for sub_tag, sub_value in value.items():
            subtree = make_xml_tree(sub_tag, sub_value)
            if isinstance(subtree, list):
                for sub in subtree:
                    element.append(sub)
            else:
                element.append(subtree)
        return element

    if isinstance(value, list):
        children = []
        for child in value:
            children.append(make_xml_tree(tag, child))
        return children

    elem = etree.Element(tag)
    elem.text = str(value)

    return elem


def to_xml_response(root_name, body):
    """Convert `body` into an XML response nested under `root_name` element"""
    xml_response = make_xml_tree(root_name, body)
    xml_response.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")

    return etree.tostring(xml_response, xml_declaration=True)


def get_key(url):
    """
    Given a URL path convert it to an S3 Key by replacing some parts of the path
    with empty strings
    """
    return url.replace("/v0/submission/", "").replace("/files", "")


@httmock.urlmatch(
    netloc="localhost", method="POST", path="/v0/submission/GDC/MISC/files/.*"
)
def handle_post_mp(url, req):
    """Handle POST requests to S3

    Mainly handle "initiate_multipart" and "complete_multipart" uploads. The first
    one will always have an "uploads" query param, the latter will have "uploadId"
    and the body will contain information about completed parts
    """
    client = boto3.client("s3")
    key = get_key(url.path)

    parsed_qs = parse_qs(url.query, keep_blank_values=True)

    # Handle "initiate_multipart" request
    if "uploads" in parsed_qs:
        result = client.create_multipart_upload(Bucket="test-bucket", Key=key)

        return httmock.response(
            result["ResponseMetadata"]["HTTPStatusCode"],
            to_xml_response("InitiateMultipartUploadResult", result),
        )

    if "uploadId" in parsed_qs:
        upload_id = parsed_qs["uploadId"][0]
        mp_request = etree.fromstring(req.body)

        # Handle "complete_multipart" request
        if mp_request.tag == "CompleteMultipartUpload":
            parts = []
            for elem in mp_request.findall("Part"):
                ordered_dict = parse(etree.tostring(elem))
                part_meta = ordered_dict["Part"]
                parts.append(
                    {
                        "ETag": part_meta["ETag"],
                        "PartNumber": int(part_meta["PartNumber"]),
                    }
                )

            result = client.complete_multipart_upload(
                Bucket="test-bucket",
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            return httmock.response(
                result["ResponseMetadata"]["HTTPStatusCode"],
                to_xml_response("CompleteMultipartUploadResult", result),
            )

        return httmock.response(400, "unable to process uploadId request")

    return httmock.response(400, "cannot process request")


@httmock.urlmatch(
    netloc="localhost", method="GET", path="/v0/submission/GDC/MISC/files/.*"
)
def handle_list_mp(url, _):
    """Handle GET requests to S3

    List parts for a given multipart uploadId
    """
    client = boto3.client("s3")
    key = get_key(url.path)

    parsed_qs = parse_qs(url.query)

    if "uploadId" not in parsed_qs:
        return httmock.response(400, "Cannot process request")

    upload_id = parsed_qs["uploadId"][0]

    result = client.list_parts(Bucket="test-bucket", Key=key, UploadId=upload_id,)

    if "Parts" in result:
        result["Part"] = result.pop("Parts")

    return httmock.response(
        result["ResponseMetadata"]["HTTPStatusCode"],
        to_xml_response("ListPartsResult", result),
    )


@httmock.urlmatch(
    netloc="localhost", method="PUT", path="/v0/submission/GDC/MISC/files/.*"
)
def handle_put_mp(url, req):
    """Handle PUT requests to S3

    Handle "upload_part" request:
        * uploadId and partNumber are required
    """
    client = boto3.client("s3")
    key = get_key(url.path)

    parsed_qs = parse_qs(url.query)

    if "uploadId" not in parsed_qs:
        return httmock.response(400, "uploadId missing")

    if "partNumber" not in parsed_qs:
        return httmock.response(400, "partNumber missing")

    upload_id = parsed_qs["uploadId"][0]
    part_number = parsed_qs["partNumber"][0]

    result = client.upload_part(
        Bucket="test-bucket",
        Key=key,
        UploadId=upload_id,
        PartNumber=int(part_number),
        Body=req.body,
    )

    return httmock.response(
        result["ResponseMetadata"]["HTTPStatusCode"],
        to_xml_response("UploadPartResult", result),
    )


@httmock.urlmatch(
    netloc="localhost", method="PUT", path="/v0/submission/GDC/MISC/files/.*"
)
def handle_put_simple(url, req):
    """Handle PUT requests to S3

    Handle simple upload requests
    """
    client = boto3.client("s3")
    key = get_key(url.path)

    # for simple upload, return 200 for initial dry_run request
    if "dry_run" in url.path:
        return httmock.response(200, "OK")

    # simple upload
    data = req.body.read(req.body.filesize)
    client.put_object(Body=data, Bucket="test-bucket", Key=key)
    return httmock.response(200, "OK")


@httmock.urlmatch(netloc="localhost", method="DELETE")
def handle_delete_mp(url, _):
    client = boto3.client("s3")
    key = get_key(url.path)

    parsed_qs = parse_qs(url.query)

    upload_id = parsed_qs["uploadId"][0]
    result = client.abort_multipart_upload(
        Bucket="test-bucket", Key=key, UploadId=upload_id,
    )

    return httmock.response(
        result["ResponseMetadata"]["HTTPStatusCode"],
        to_xml_response("AbortMultipartUploadResponse", result),
    )


@pytest.fixture
def s3_proxy_handlers_mp(mock_s3_bucket, mock_s3_conn):
    return handle_post_mp, handle_list_mp, handle_put_mp, handle_delete_mp


@pytest.fixture
def s3_proxy_handlers_simple(mock_s3_bucket, mock_s3_conn):
    return handle_put_simple


@pytest.fixture
def mock_submission_server_mp(mock_graphql_responses, s3_proxy_handlers_mp):
    with httmock.HTTMock(mock_graphql_responses, *s3_proxy_handlers_mp):
        yield


@pytest.fixture
def mock_submission_server_simple(mock_graphql_responses, s3_proxy_handlers_simple):
    with httmock.HTTMock(mock_graphql_responses, s3_proxy_handlers_simple):
        yield


@pytest.fixture
def complete_multipart_side_effect(monkeypatch):
    def complete_side_effect(*_, **__):
        raise Exception("Interrupt completion")

    monkeypatch.setattr(client.GDCUploadClient, "complete", complete_side_effect)


@pytest.fixture
def upload_multipart_side_effect(monkeypatch):
    def upload_multipart_side_effect(*_, **__):
        return False

    monkeypatch.setattr(client, "upload_multipart", upload_multipart_side_effect)


@pytest.fixture
def _upload_side_effect(monkeypatch):
    def _upload_side_effect(*_, **__):
        return

    monkeypatch.setattr(client.GDCUploadClient, "_upload", _upload_side_effect)


@pytest.fixture(autouse=True)
def cleanup_resume():
    yield

    if os.path.isfile("resume_None"):
        os.remove("resume_None")


@pytest.fixture
def s3_client(mock_s3_conn, mock_s3_bucket):
    return boto3.client("s3")


def assert_common_unsuccessful_scenario(s3_client, client):
    assert len(client.incompleted) == 0
    with pytest.raises(Exception, match=".*(NoSuchKey).*"):
        s3_client.get_object(Bucket="test-bucket", Key="GDC/MISC/file-id-1")

    with pytest.raises(Exception, match=".*(NoSuchKey).*"):
        s3_client.get_object(Bucket="test-bucket", Key="GDC/MISC/file-id-2")

    assert len(client.file_entities) == 2
    assert len(client._metadata) == 2


def assert_mp_unsuccessful_scenario(s3_client, client):
    assert os.path.isfile("resume_None")
    assert_common_unsuccessful_scenario(s3_client, client)


def assert_successful_scenario(s3_client, client):
    assert len(client.file_entities) == 2
    assert len(client._metadata) == 2
    assert not os.path.isfile("resume_None")

    # Make sure the objects exist
    obj1 = s3_client.get_object(Bucket="test-bucket", Key="GDC/MISC/file-id-1")
    obj2 = s3_client.get_object(Bucket="test-bucket", Key="GDC/MISC/file-id-2")
    # Make sure content is correct
    assert obj1["Body"].read(64).decode() == "a" * 64
    assert obj2["Body"].read(64).decode() == "b" * 64


def test_create_resume_path():
    # don't need to test if there's no file given
    # that is checked in multipart_upload()
    tests = ["/path/to/file.yml", "path/to/file.yml", "file.yml"]
    results = [
        "/path/to/resume_file.yml",
        "path/to/resume_file.yml",
        "resume_file.yml",
    ]
    for i, t in enumerate(tests):
        assert client.create_resume_path(t) == results[i]


@pytest.mark.usefixtures(
    "mock_files", "mock_submission_server_simple", "mock_simple_upload_client"
)
def test_simple_upload__success(s3_client, mock_simple_upload_client):
    mock_simple_upload_client.upload()
    assert_successful_scenario(s3_client, mock_simple_upload_client)


@pytest.mark.usefixtures(
    "mock_files", "mock_submission_server_mp", "mock_mp_upload_client"
)
def test_mp_upload__success(s3_client, mock_mp_upload_client):
    mock_mp_upload_client.upload()
    assert_successful_scenario(s3_client, mock_mp_upload_client)


@pytest.mark.usefixtures(
    "mock_files",
    "mock_submission_server_simple",
    "mock_simple_upload_client",
    "_upload_side_effect",
)
def test_simple_upload__unsuccessful(s3_client, mock_simple_upload_client):
    mock_simple_upload_client.upload()
    assert_common_unsuccessful_scenario(s3_client, mock_simple_upload_client)


@pytest.mark.usefixtures(
    "mock_files",
    "mock_submission_server_mp",
    "mock_mp_upload_client",
    "complete_multipart_side_effect",
)
def test_mp_upload__complete_call_failed(s3_client, mock_mp_upload_client):
    mock_mp_upload_client.upload()
    assert_mp_unsuccessful_scenario(s3_client, mock_mp_upload_client)


@pytest.mark.usefixtures(
    "mock_files",
    "mock_submission_server_mp",
    "mock_mp_upload_client",
    "upload_multipart_side_effect",
)
def test_mp_upload__upload_multipart_call_failed(s3_client, mock_mp_upload_client):
    mock_mp_upload_client.upload()
    assert_mp_unsuccessful_scenario(s3_client, mock_mp_upload_client)
