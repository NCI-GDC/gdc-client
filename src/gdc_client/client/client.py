from contextlib import closing
from contextlib import contextmanager

import requests

from gdc_client import auth
from gdc_client import version


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

    @contextmanager
    def request(self, verb, path, **kwargs):
        """Make a request to the GDC API."""
        res = self.session.request(
            verb,
            "https://{host}:{port}{path}".format(
                host=self.host,
                port=self.port,
                path=path,
            ),
            auth=auth.GDCTokenAuth(self.token),
            **kwargs,
        )

        with closing(res):
            yield res

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
