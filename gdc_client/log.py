import logging
import sys

loggers = {}


# Logging
def get_logger(name='parcel'):
    """Create or return an existing logger with given name
    """

    if name in loggers:
        return loggers[name]
    log = logging.getLogger(name)
    log.propagate = False
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    log.addHandler(handler)
    loggers[name] = log
    return log
