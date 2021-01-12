"""gdc_client.query.versions

Functionality related to versioning.
"""
import logging
import requests

from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)


def get_latest_versions(url, uuids, verify=True):
    """Get the latest version of a UUID according to the api.

    Args:
        url (str):
        uuids (list): list of UUIDs that might have a new version

    Returns:
        dict: mapping for user requested file UUIDs potentially new versions
    Raises:
        ServerError: if request for files versions fails
    """

    uuids = list(uuids)
    versions_url = url + "/files/versions"
    latest_versions = {}

    # Make multiple queries in an attempt to balance the load on the server.
    for chunk in _chunk_list(uuids):
        resp = requests.post(versions_url, json={"ids": chunk}, verify=verify)

        if not resp.ok:
            raise HTTPError(
                (
                    "The following request {0} for ids {1} returned with "
                    "status code: {2} and response content: {3}"
                ).format(versions_url, chunk, resp.status_code, resp.content,),
                response=resp,
            )

        # Parse the results of the chunked query.
        for result in resp.json():
            file_id = result.get("id")
            uuid = result.get("latest_id")
            if uuid:
                latest_versions[file_id] = uuid
            else:
                # Might happen for legacy files
                latest_versions[file_id] = file_id

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
        yield elements[current_index : current_index + chunk_size]
        current_index += chunk_size
