#!/usr/bin/env bash

# Make sure the correct version of pyinstall is installed
pip install 'PyInstaller==2.1'

# Get version
VERSION=$(python -c """
import gdc_client.version
print gdc_client.version.__version__
""")

# Create binary
pyinstaller --clean --noconfirm --onefile -c gdc-client

APPNAME="gdc-client"
SOURCE="dist/gdc-client"

# Zip dist
zip "gdc-client_${VERSION}_Ubuntu14.04_x64.zip" "${SOURCE}"
