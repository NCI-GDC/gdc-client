import argparse
from version import __version__ as version

args = argparse.ArgumentParser(description=('GDC Command Line Client'))
argparser = argparse.ArgumentParser()
argparser.add_argument('--version', '-v', action='version',
                       version='%(prog)s {}'.format(version))

subparsers = argparser.add_subparsers(help='sub-command help', dest='command')
