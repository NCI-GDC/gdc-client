import jsonschema
import yaml

from gdc_client.upload.schema import UPLOAD_MANIFEST_SCHEMA
from gdc_client.upload.exceptions import ValidationError


def validate(manifest, schema=UPLOAD_MANIFEST_SCHEMA):
    """ Validate a manifest against the current schema.
    """
    try:
        jsonschema.validate(manifest, schema)
    except jsonschema.ValidationError as err:
        raise ValidationError(err)


def load(m, schema=UPLOAD_MANIFEST_SCHEMA):
    """ Load and validate a manifest.
    """
    manifest = yaml.load(m)

    validate(
        manifest, schema=schema,
    )

    return manifest
