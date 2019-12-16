import logging
import sys

from gdc_client.parcel import colored

class LogFormatter(logging.Formatter):

    err_format  = colored('ERROR: ', 'red') + '%(msg)s'
    warn_format = colored('WARNING: ', 'yellow') + '%(msg)s'
    dbg_format  = colored('%(asctime)s - DEBUG: %(module)s: %(lineno)d: ', 'blue') + '%(msg)s'
    info_format = '%(msg)s'


    def __init__(self, fmt='%(asctime)s - %(levelname)s: %(msg)s', style='%',
                 color_off=False):
        logging.Formatter.__init__(self, fmt)
        self.color_off = color_off


    def format(self, record):
        # Skip colored output if the flag is set
        # TODO: color off does not respect other formatting
        if self.color_off:
            return logging.Formatter.format(self, record)

        # Save the original format
        format_orig = self._style._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.DEBUG:
            self._style._fmt = LogFormatter.dbg_format

        elif record.levelno == logging.INFO:
            self._style._fmt = LogFormatter.info_format

        elif record.levelno == logging.WARNING:
            self._style._fmt = LogFormatter.warn_format

        elif record.levelno == logging.ERROR:
            self._style._fmt = LogFormatter.err_format

        result = logging.Formatter.format(self, record)

        # Restore the original format
        self._style._fmt = format_orig

        return result

