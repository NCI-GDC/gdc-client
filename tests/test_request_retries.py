from unittest import mock

import pytest
import requests
import requests_mock

from gdc_client import exceptions
from gdc_client.common import config
from gdc_client.query import versions


@pytest.fixture
def retry_sleep(monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
    """Record retry delays without actually sleeping."""
    sleep = mock.Mock()
    monkeypatch.setattr(versions._post_versions.retry, "sleep", sleep)
    return sleep


@pytest.mark.parametrize(
    "error_type",
    [
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        RuntimeError,
    ],
)
def test_version_query_recovers(
    retry_sleep: mock.Mock,
    error_type: type[Exception],
) -> None:
    with requests_mock.Mocker() as http_mock:
        http_mock.post(
            "https://example.com/files/versions",
            [
                {"exc": error_type("temporary failure")},
                {"json": [{"id": "old", "latest_id": "new"}]},
            ],
        )

        result = versions.get_latest_versions("https://example.com", ["old"])

        assert result == {"old": "new"}
        assert http_mock.call_count == 2
        for request in http_mock.request_history:
            assert request.json() == {"ids": ["old"]}
            assert request.timeout == (
                config.REQUEST_CONNECT_TIMEOUT,
                config.REQUEST_READ_TIMEOUT,
            )
            assert request.verify is True

    retry_sleep.assert_called_once_with(1)


def test_version_query_stops_after_five_attempts(
    monkeypatch: pytest.MonkeyPatch,
    retry_sleep: mock.Mock,
) -> None:
    failure = requests.exceptions.ConnectionError("still offline")
    post = mock.Mock(side_effect=failure)
    monkeypatch.setattr(versions.requests, "post", post)

    with pytest.raises(requests.exceptions.ConnectionError) as caught:
        versions.get_latest_versions("https://example.com", ["old"])

    assert caught.value is failure
    assert post.call_count == 5
    assert retry_sleep.call_args_list == [
        mock.call(1),
        mock.call(2),
        mock.call(4),
        mock.call(8),
    ]


@pytest.mark.parametrize(
    "error_type",
    [exceptions.ClientError, requests.exceptions.HTTPError],
)
def test_version_query_does_not_retry_excluded_errors(
    monkeypatch: pytest.MonkeyPatch,
    retry_sleep: mock.Mock,
    error_type: type[Exception],
) -> None:
    failure = error_type("permanent failure")
    post = mock.Mock(side_effect=failure)
    monkeypatch.setattr(versions.requests, "post", post)

    with pytest.raises(error_type) as caught:
        versions.get_latest_versions("https://example.com", ["old"])

    assert caught.value is failure
    post.assert_called_once()
    retry_sleep.assert_not_called()
