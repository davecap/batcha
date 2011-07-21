__all__ = ['Analysis']

import logging
# see the advice on logging and libraries in
# http://docs.python.org/library/logging.html?#configuring-logging-for-a-library
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
h = NullHandler()
logging.getLogger("batcha").addHandler(h)
del h

def start_logging(logfile="batcha.log"):
    """Start logging of messages to file and console.

    The default logfile is named `batcha.log` and messages are
    logged with the tag *batcha*.
    """
    import log
    core.log.create("batcha", logfile=logfile)
    logging.getLogger("batcha").info("batcha STARTED logging to %r", logfile)

def stop_logging():
    """Stop logging to logfile and console."""
    import log
    logging.getLogger("batcha").info("batcha STOPPED logging")
    core.log.clear_handlers(logger)  # this _should_ do the job...

from analysis import Analysis
