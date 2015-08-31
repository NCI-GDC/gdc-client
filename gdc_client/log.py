import logging
import sys

from parcel import colored

loggers = {}


# Logging
def get_logger(name='gdc-client'):
    """Create or return an existing logger with given name
    """

    if name in loggers:
        return loggers[name]
    log = logging.getLogger(name)
    log.propagate = False
    if sys.stdout.isatty():
        formatter = logging.Formatter(
            colored('%(asctime)s: %(levelname)s: ', 'blue')+'%(message)s')
    else:
        formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    log.addHandler(handler)
    loggers[name] = log
    return log
