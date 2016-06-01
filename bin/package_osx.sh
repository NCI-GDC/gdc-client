#!/usr/bin/env bash

set -e
set -x

# Make sure the correct version of pyinstall is installed
pip install 'PyInstaller==2.1'

# Make sure the correct version of setuptools is present
value=$(pip list | grep setuptools)
echo $value
version="19.2"
if [[ $value != *$version* ]]
then
    echo "Incorrect version:"
    echo $value
    echo "please install $version"
    exit 1
fi

# Get version
VERSION=$(python -c """
import gdc_client.version
print gdc_client.version.__version__
""")

echo "Creating package for version ${VERSION}"

# Create binary
pyinstaller --clean --additional-hooks-dir=. --noconfirm --onefile -c gdc-client

# Zip dist
zip "gdc-client_${VERSION}_OSX_x64.zip" gdc-client
