#!/usr/bin/env bash

set -x

# Intended to be run with Windows git (MINGW32)

# Make sure the correct version of pyinstall is installed
/c/Python27/Scripts/pip.exe install PyInstaller==2.1

# Create binary
/c/Python27/Scripts/pyinstaller -F --clean --noconfirm --onefile -c gdc-client -i ../resources/gdc_client.ico