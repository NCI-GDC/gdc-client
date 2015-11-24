#!/usr/bin/env bash

set -e
set -x

# Make sure the correct version of pyinstall is installed
pip install 'PyInstaller==2.1'

# Get version
VERSION=$(python -c """
import gdc_client.version
print gdc_client.version.__version__
""")

echo "Creating package for version ${VERSION}"

# Create binary
pyinstaller --clean --additional-hooks-dir=. --noconfirm --onefile -c gdc-client

# Bundle app
APPNAME="gdc-client"

BUNDLE="${APPNAME}.app"
SOURCE="gdc-client"
ICON="../../resources/gdc_client.icns"
CONTENTS="${BUNDLE}/Contents"
MacOS="${CONTENTS}/MacOS"
RESOURCES="${CONTENTS}/Resources"
WRAPPER="${MacOS}/${APPNAME}"

cd "dist"

if [ -a "${BUNDLE}" ]; then
    rm -rf "${BUNDLE}"
fi

echo "Bundling to ${BUNDLE}"

# Create bundle
mkdir -p "${MacOS}"

# Create wrapper script
echo '#!/bin/bash
BIN="$(cd "$(dirname "$0")"; pwd)/.gdc-client"
chmod +x "${BIN}"
open -a Terminal "${BIN}"
' > "${WRAPPER}"
chmod +x "${WRAPPER}"

# Move binary into bundle
cp "${SOURCE}" "${MacOS}/.${APPNAME}"

# Add icon
mkdir "${RESOURCES}"
cp "${ICON}" "${RESOURCES}/"
echo '
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist SYSTEM
"file://localhost/System/Library/DTDs/PropertyList.dtd">
<plist version="0.9">
<dict>
   <key>CFBundleIconFile</key>
   <string>gdc_client.icns</string>
</dict>
</plist>
' > "${CONTENTS}/Info.plist"

# Zip dist
zip "gdc-client_${VERSION}_OSX_x64.zip" "${BUNDLE}" "${SOURCE}"
