#!/usr/bin/env bash

set -e

# Make sure the correct version of pyinstall is installed
pip install https://github.com/pyinstaller/pyinstaller/archive/develop.zip


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
