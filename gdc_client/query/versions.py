"""gdc_client.query.versions

Functionality related to versioning.
"""
import logging

import requests


logger = logging.getLogger(__name__)


def get_latest_versions(url, uuids):
    """Get the latest version of a UUID according to the api.

    Args:
        url (str):
        uuids (list): list of UUIDs that might have a new version

    Returns:
        list: list of uuids with potentially new versions
    """

    uuids = list(uuids)
    versions_url = url + '/files/versions'
    latest_versions = []

    # Make multiple queries in an attempt to balance the load on the server.
    for chunk in _chunk_list(uuids):
        resp = requests.post(versions_url, json={'ids': chunk})

        # Parse the results of the chunked query.
        for result in resp.json():
            # Probably wouldn't happen, but just in case add an error check.
            file_id = result.get("id")
            uuid = result.get('latest_id')
            if uuid:
                logger.info("Latest version for {} ==> {}".format(file_id, uuid))
                latest_versions.append(uuid)
            logger.info("No latest version found for {}".format(file_id))

    return latest_versions


def _chunk_list(elements, chunk_size=500):
    """Generator to chunk any list into smaller parts.

    Args:
        elements (list): list of elements
        chunk_size (int): max number of elements in the resulting chunk

    Returns:
        generator: generator yielding chunked workloads
    """

    current_index = 0
    while current_index < len(elements):
        yield elements[current_index:current_index + chunk_size]
        current_index += chunk_size
