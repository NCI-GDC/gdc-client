from gdc_client.exceptions import ClientError


class ValidationError(ClientError):
    """ Base validation error.
    """
