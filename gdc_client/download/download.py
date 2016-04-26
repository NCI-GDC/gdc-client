import logging

import requests

from gdc_client.client import GDCClient


class DownloadClient(GDCClient):
    """ GDC Download Requests Client
    """
    def __init__(self, stream=True, verify=True, **kwargs):
        super(DownloadClient, self).__init__(**kwargs)

        self.session.stream = stream
        self.session.verify = verify

    # TODO FIXME MOVE THIS ELSEWHERE
    def info(self, uuid, **kwargs):
        """ Get info about a file by uuid from the GDC.
        """
        logging.debug('looking up {uuid}'.format(uuid=uuid))

        RESOURCE = '/data/{uuid}'
        resource = RESOURCE.format(uuid=uuid)

        context = super(DownloadClient, self).head(resource, **kwargs)

        with context as res:
            res.raise_for_status()

            info = {
                'size': res.headers.get('Content-Length', None),
            }

        return info

    def download(self, uuid, **kwargs):
        """ Download a file by uuid from the GDC.

        Excess keyword arguments are passed to the request.

        Returns a generator of bytes.
        """
        logging.debug('downloading {uuid}'.format(uuid=uuid))

        RESOURCE = '/data/{uuid}'
        resource = RESOURCE.format(uuid=uuid)

        context = super(DownloadClient, self).get(resource, **kwargs)

        with context as res:
            # TODO replace w/ ClientError
            res.raise_for_status()
            for chunk in res.iter_content(1024):
                yield chunk

    def download_to_file(self, uuid, ofs, **kwargs):
        """ Download a file by uuid from the GDC to a file.

        Excess keyword arguments are passed to the request.
        """
        logging.debug('downloading {uuid} to {ofs}'.format(uuid=uuid, ofs=ofs))
        for chunk in self.download(uuid):
            ofs.write(chunk)

