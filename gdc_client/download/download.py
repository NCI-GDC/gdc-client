import cgi
import logging

import requests

from gdc_client.utils import sizefmt
from gdc_client.client import GDCClient
from gdc_client.exceptions import ClientError


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
            try: res.raise_for_status()
            except requests.HTTPError as err:
                raise ClientError(err)

            length = res.headers.get('Content-Length', None)
            length = None if length is None else int(length)

            disposition = res.headers.get('Content-Disposition', '')
            val, params = cgi.parse_header(disposition)
            if val != 'attachment':
                log.warning('received non-attachment response')

            filename = params.get('filename', None)

        info = {
            'size': length,
            'name': filename,
        }

        return info

    def download(self, uuid, **kwargs):
        """ Download a file by uuid from the GDC.

        Excess keyword arguments are passed to the request.

        Returns a generator of bytes.
        """
        RESOURCE = '/data/{uuid}'
        resource = RESOURCE.format(uuid=uuid)

        info = self.info(uuid)

        name = info['name']
        size = info['size']

        logging.info('Starting download: {uuid}'.format(uuid=uuid))
        logging.info('Reported filename: {name}'.format(name=name))
        logging.info('Reported filesize: {size}'.format(size=sizefmt(size)))

        context = super(DownloadClient, self).get(resource, **kwargs)

        written = 0

        with context as res:
            try: res.raise_for_status()
            except requests.HTTPError as err:
                raise ClientError(err)

            for chunk in res.iter_content(1024):
                written += len(chunk)
                yield chunk

        if size is not None and written != size:
            err = 'received {received} of {reported} bytes'.format(
                received=written,
                written=size,
            )
            raise ClientError(err)

    def download_to_file(self, uuid, ofs, **kwargs):
        """ Download a file by uuid from the GDC to a file.

        Excess keyword arguments are passed to the request.
        """
        logging.info('Downloading to: {name}'.format(name=ofs.name))

        for chunk in self.download(uuid):
            ofs.write(chunk)

