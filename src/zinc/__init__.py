
from .errors import ZincErrors
from .helpers import *
from .defaults import defaults

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

if __name__ == "__main__":
    import zinc.cli
    zinc.cli.main()
