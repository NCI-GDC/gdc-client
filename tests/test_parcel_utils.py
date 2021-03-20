import logging
from unittest import mock

import pytest

from gdc_client.parcel import utils
from gdc_client import exceptions


@mock.patch("gdc_client.parcel.utils.md5sum_whole_file")
def test__validate_file_md5sum(mock_md5sum_whole_file):
    stream = mock.MagicMock()
    check_file_md5sum_mock = mock.PropertyMock(return_value=True)
    is_regular_file_mock = mock.PropertyMock(return_value=True)
    md5sum_mock = mock.PropertyMock(
        return_value="d47b127bc2de2d687ddc82dac354c415"  # pragma: allowlist secret
    )
    type(stream).check_file_md5sum = check_file_md5sum_mock
    type(stream).is_regular_file = is_regular_file_mock
    type(stream).md5sum = md5sum_mock
    mock_md5sum_whole_file.return_value = (
        "d47b127bc2de2d687ddc82dac354c415"  # pragma: allowlist secret
    )
    file_path = "test.txt"

    utils.validate_file_md5sum(stream, file_path)

    assert check_file_md5sum_mock.call_count == 1
    assert is_regular_file_mock.call_count == 1
    assert md5sum_mock.call_count == 2


def test__validate_file_md5sum_negative_validate_disabled(caplog):
    stream = mock.MagicMock()
    check_file_md5sum_mock = mock.PropertyMock(return_value=False)
    type(stream).check_file_md5sum = check_file_md5sum_mock
    file_path = "test.txt"

    with caplog.at_level(logging.DEBUG):
        utils.validate_file_md5sum(stream, file_path)

        check_file_md5sum_mock.assert_called_once_with()
        assert caplog.records[0].message == "checksum validation disabled"


@pytest.mark.parametrize(
    "properties, expected",
    [
        (dict(check_file_md5sum=True, is_regular_file=False), "Not a regular file"),
        (
            dict(check_file_md5sum=True, is_regular_file=True, md5sum=None),
            (
                "Cannot validate this file since the server "
                "did not provide an md5sum. Use the "
                "'--no-file-md5sum' option to ignore this error."
            ),
        ),
        (
            dict(
                check_file_md5sum=True,
                is_regular_file=True,
                md5sum="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            "File checksum is invalid",
        ),
    ],
    ids=["not_regular_file", "no_server_md5sum", "incorrect_md5sum"],
)
@mock.patch("gdc_client.parcel.utils.md5sum_whole_file")
def test__validate_file_md5sum_negative_validation_errors(
    mock_md5sum_whole_file, properties: dict, expected: str
):
    stream = mock.MagicMock(**properties)
    file_path = "test.txt"
    mock_md5sum_whole_file.return_value = (
        "d47b127bc2de2d687ddc82dac354c415"  # pragma: allowlist secret
    )

    with pytest.raises(exceptions.MD5ValidationError, match=r"{}".format(expected)):
        utils.validate_file_md5sum(stream, file_path)


def test__md5sum_whole_file():
    with mock.patch(
        "builtins.open", mock.mock_open(read_data=b"A" * 1024)
    ) as mock_file:
        assert utils.md5sum_whole_file("test.txt") == (
            "d47b127bc2de2d687ddc82dac354c415"  # pragma: allowlist secret
        )
        mock_file.assert_called_once_with("test.txt", "rb")
