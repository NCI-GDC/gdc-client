# ***************************************************************************************
# Title: LabAdvComp/parcel
# Author: Joshua S. Miller
# Date: May 26, 2016
# Code version: 0.1.13
# Availability: https://github.com/LabAdvComp/parcel
# ***************************************************************************************

import csv
import argparse


def parse(fd, delimiter='\t', quotechar='#', **kwargs):
    """Parses a manifest file string.

    :param fd:
        A file-like object containing a GDC manifest
    """

    manifest = csv.DictReader(
        fd, delimiter=delimiter, quotechar=quotechar)

    for row in manifest:
        yield row


argparse_type = lambda x: parse(argparse.FileType('r')(x))
