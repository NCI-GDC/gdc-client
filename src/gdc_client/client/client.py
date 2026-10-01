import collections.abc
import contextlib
import types
from typing import Any

import requests
import tenacity

from gdc_client import auth, exceptions, version
from gdc_client.common import config

GDC_API_HOST = "api.gdc.cancer.gov"
GDC_API_PORT = 443


class GDCClient:
    """GDC API Requests Client"""

    def __init__(self, host=GDC_API_HOST, port=GDC_API_PORT, token=None):
        self.host = host
        self.port = port
        self.token = token

        self.session = requests.Session()

        agent = " ".join(
            [
                f"GDC-Client/{version.__version__}",
                self.session.headers.get("User-Agent", "Unknown"),
            ]
        )

        self.session.headers = {
            "User-Agent": agent,
        }

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.session.close()

    @tenacity.retry(
        retry=tenacity.retry_if_not_exception_type(
            (exceptions.ClientError, requests.exceptions.HTTPError)
        ),
        wait=tenacity.wait_exponential(max=config.REQUEST_RETRY_MAX_WAIT),
        stop=tenacity.stop_after_attempt(config.REQUEST_RETRY_ATTEMPTS),
        reraise=True,
    )
    def _send_request(
        self,
        verb: str,
        url: str,
        auth_handler: requests.auth.AuthBase,
        request_options: dict[str, Any],
    ) -> requests.Response:
        return self.session.request(verb, url, auth=auth_handler, **request_options)

    @contextlib.contextmanager
    def request(
        self, verb: str, path: str, **kwargs: Any
    ) -> collections.abc.Iterator[requests.Response]:
        """Make a request to the GDC API."""
        url = f"https://{self.host}:{self.port}{path}"
        auth_handler = auth.GDCTokenAuth(self.token)

        request_options = {
            "timeout": (
                config.REQUEST_CONNECT_TIMEOUT,
                config.REQUEST_READ_TIMEOUT,
            )
        }
        request_options.update(kwargs)

        with self._send_request(verb, url, auth_handler, request_options) as response:
            yield response

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)

    def put(self, path, **kwargs):
        return self.request("PUT", path, **kwargs)

    def post(self, path, **kwargs):
        return self.request("POST", path, **kwargs)

    def head(self, path, **kwargs):
        return self.request("HEAD", path, **kwargs)

    def patch(self, path, **kwargs):
        return self.request("PATCH", path, **kwargs)

    def delete(self, path, **kwargs):
        return self.request("DELETE", path, **kwargs)
