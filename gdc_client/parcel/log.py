# ***************************************************************************************
# Title: LabAdvComp/parcel
# Author: Joshua S. Miller
# Date: May 26, 2016
# Code version: 0.1.13
# Availability: https://github.com/LabAdvComp/parcel
# ***************************************************************************************

import logging
import sys

from gdc_client.parcel.portability import colored


loggers = {}


# Logging
def get_logger(name="parcel"):
    """Create or return an existing logger with given name
    """

    if name in loggers:
        return loggers[name]
    log = logging.getLogger(name)
    log.propagate = False
    if sys.stderr.isatty():
        formatter = logging.Formatter(
            colored("%(asctime)s: %(levelname)s: ", "blue") + "%(message)s"
        )
    else:
        formatter = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    log.addHandler(handler)
    loggers[name] = log
    return log
