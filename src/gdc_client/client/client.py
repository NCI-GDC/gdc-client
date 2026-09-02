from contextlib import contextmanager

import requests

from gdc_client import auth, version

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

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    @contextmanager
    def request(self, verb, path, **kwargs):
        """Make a request to the GDC API."""
        url = f"https://{self.host}:{self.port}{path}"
        auth_handler = auth.GDCTokenAuth(self.token)

        with self.session.request(verb, url, auth=auth_handler, **kwargs) as res:
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
