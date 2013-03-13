
from .errors import ZincErrors
from .helpers import *
from .defaults import defaults

import logging
log = logging.getLogger(__name__)

if __name__ == "__main__":
    import zinc.cli
    zinc.cli.main()
