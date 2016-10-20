import logging
import sys

from parcel import colored

loggers = {}

class LogFormatter(logging.Formatter):

        err_format  = colored('ERROR: ', 'red') + '%(msg)s'
        warn_format  = colored('WARNING: ', 'yellow') + '%(msg)s'
        dbg_format  = colored('%(asctime)s - DEBUG: %(module)s: %(lineno)d: ', 'blue') + '%(msg)s'
        info_format = '%(asctime)s - INFO: %(msg)s'


        def __init__(self, fmt='%(asctime)s - %(levelname)s: %(msg)s'):
            logging.Formatter.__init__(self, fmt)


        def format(self, record):

            # Save the original format
            format_orig = self._fmt

            # Replace the original format with one customized by logging level
            if record.levelno == logging.DEBUG:
                self._fmt = LogFormatter.dbg_format

            elif record.levelno == logging.INFO:
                self._fmt = LogFormatter.info_format

            elif record.levelno == logging.WARNING:
                self._fmt = LogFormatter.warn_format

            elif record.levelno == logging.ERROR:
                self._fmt = LogFormatter.err_format

            result = logging.Formatter.format(self, record)

            # Restore the original format
            self._fmt = format_orig

            return result

# Logging
def get_logger(name='gdc-client'):
    """Create or return an existing logger with given name
    """

    if name in loggers:
        return loggers[name]
    log = logging.getLogger(name)
    log.propagate = False
    if sys.stderr.isatty():
        formatter = LogFormatter()
    else:
        formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    log.addHandler(handler)
    loggers[name] = log
    return log
