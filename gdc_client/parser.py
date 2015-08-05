import argparse
from version import __version__ as version

parser = argparse.ArgumentParser(description=('GDC Command Line Client'))
parser = argparse.ArgumentParser()
parser.add_argument('--version', '-v', action='version',
                    version='%(prog)s {}'.format(version))

subparsers = parser.add_subparsers(help='sub-command help', dest='command')
