
from .errors import ZincErrors
from .helpers import *
from .defaults import defaults

import logging
logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s')

if __name__ == "__main__":
    import zinc.cli
    zinc.cli.main()
