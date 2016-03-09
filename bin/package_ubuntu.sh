#!/usr/bin/env bash

export PYTHONPATH=.:$PYTHONPATH
set -e

# Make sure the correct version of pyinstall is installed
pip install "https://github.com/pyinstaller/pyinstaller/archive/1e38dcb5916f3fc22089e169ff1ea61c05d66ad0.zip" 

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

# Create binary
pyinstaller --clean --additional-hooks-dir=. --noconfirm --onefile -c gdc-client

# Zip dist
SOURCE_DIR='dist'
SOURCE="gdc-client"
ZIP_DIR="${PWD}"
cd "${SOURCE_DIR}"

zip "${ZIP_DIR}/gdc-client_${VERSION}_Ubuntu14.04_x64.zip" "${SOURCE}"
