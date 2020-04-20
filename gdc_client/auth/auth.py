import requests


class GDCTokenAuth(requests.auth.AuthBase):
    """ GDC Token Authentication
    """

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["X-Auth-Token"] = self.token
        return r
