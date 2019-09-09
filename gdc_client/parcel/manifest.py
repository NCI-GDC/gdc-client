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
