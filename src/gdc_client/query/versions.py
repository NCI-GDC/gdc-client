"""gdc_client.query.versions

Functionality related to versioning.
"""

import logging

import requests
import tenacity

from gdc_client import exceptions
from gdc_client.common import config

logger = logging.getLogger(__name__)


@tenacity.retry(
    retry=tenacity.retry_if_not_exception_type(
        (exceptions.ClientError, requests.exceptions.HTTPError)
    ),
    wait=tenacity.wait_exponential(max=config.REQUEST_RETRY_MAX_WAIT),
    stop=tenacity.stop_after_attempt(config.REQUEST_RETRY_ATTEMPTS),
    reraise=True,
)
def _post_versions(url: str, ids: list[str], verify: bool) -> requests.Response:
    return requests.post(
        url,
        json={"ids": ids},
        verify=verify,
        timeout=(config.REQUEST_CONNECT_TIMEOUT, config.REQUEST_READ_TIMEOUT),
    )


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
        with _post_versions(versions_url, chunk, verify) as response:
            if not response.ok:
                raise requests.exceptions.HTTPError(
                    (
                        f"The following request {versions_url} for ids {chunk} returned with "
                        f"status code: {response.status_code} and "
                        f"response content: {response.content}"
                    ),
                    response=response,
                )

            # Parse the results of the chunked query.
            for result in response.json():
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
