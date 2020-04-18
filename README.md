# GDC Data Transfer Tool (gdc-client)

[Overview of the GDC Data Transfer Tool](https://gdc.cancer.gov/access-data/gdc-data-transfer-tool)

The gdc-client provides several convenience functions over the GDC API which provides general download/upload via HTTPS.

## Badges:
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/bd6edccc96fe40bba154086169b3d237)](https://www.codacy.com/app/NCI-GDC/gdc-client?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=NCI-GDC/gdc-client&amp;utm_campaign=Badge_Grade)
[![Python 3](https://pyup.io/repos/github/NCI-GDC/gdc-client/python-3-shield.svg)](https://pyup.io/repos/github/NCI-GDC/gdc-client/)
[![Updates](https://pyup.io/repos/github/NCI-GDC/gdc-client/shield.svg)](https://pyup.io/repos/github/NCI-GDC/gdc-client/)

## Building the gdc-client

There is a bash script inside the ./bin directory of this repository named `package` that does most of the heavy lifting for building a single executable file of the gdc-client through PyInstaller. It will attempt to guess your operating system, based on `uname`, and build accordingly.

Building on Windows requires the installation of [git](https://git-scm.com/downloads) and the use of the git-shell that comes bundled with it. This will provide enough Unix-like utility needed to run the bash script in this repository.

### Instructions 

```bash
# The script is currently location-dependant, so navigate to the bin directory.
cd bin
# Then just execute the package script. The result will be a zip file containing your executable.
./package
```

## Executing unit tests

First install the Python package from source locally

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python setup.py install
pip install -r dev-requirements.txt
```

Run tests
- `python -m pytest tests/`

Run tests with coverage:
- `python -m pytest --cov=gdc_client --cov-branch --cov-report term tests/`

## Install `pre-commit`

This repository makes use of `pre-commit` for code formatting.
In order to make use of it, run the following command:
```
pre-commit install
```

Note: This requires you dev environment to have Python 3.6 or higher.

## Contributing

Read how to contribute [here](https://github.com/NCI-GDC/portal-ui/blob/develop/CONTRIBUTING.md)
