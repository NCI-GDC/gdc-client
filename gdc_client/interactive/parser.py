import sys
import argparse

from . import repl


def interactive(args):
    """ Initiates an interactive REPL.
    """
    # TODO use any inherited top-level args
    r = repl.GDCREPL()
    r.prompt = '\ngdc-client >> '
    r.cmdloop()

def config(parser):
    """ Configure a parser for interactive REPL.
    """
    parser.set_defaults(func=interactive)
