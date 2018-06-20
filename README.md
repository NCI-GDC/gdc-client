# GDC Data Transfer Tool (gdc-client)

[Overview of the GDC Data Transfer Tool](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)

The gdc-client provides several convenience functions over the GDC API which provides general download/upload via HTTPS.

## Badges:
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/8d050effbfa541909125ab5918c8ac41)](https://www.codacy.com/app/jbarno/gdc-client?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=NCI-GDC/gdc-client&amp;utm_campaign=Badge_Grade)
[![Python 3](https://pyup.io/repos/github/NCI-GDC/gdc-client/python-3-shield.svg)](https://pyup.io/repos/github/NCI-GDC/gdc-client/)
[![Updates](https://pyup.io/repos/github/NCI-GDC/gdc-client/shield.svg)](https://pyup.io/repos/github/NCI-GDC/gdc-client/)

## Tests

In order to run tests:

- `pip install -r requirements.txt`
- `pip install -r dev-requirements.txt`
- `python setup.py install`

Run tests
- `python -m pytest tests/`
Run tests with coverage:
- `python -m pytest --cov=gdc_client --cov-branch  --cov-report term tests/`

## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/portal-ui/blob/develop/CONTRIBUTING.md)
