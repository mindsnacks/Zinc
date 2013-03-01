
from .errors import ZincErrors
from .helpers import *
from .defaults import defaults
from .models import ZincIndex, ZincManifest, ZincFlavorSpec
from .catalog import ZincCatalog, create_catalog_at_path, ZincCatalogConfig, load_config

import logging
logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s')

if __name__ == "__main__":
    import zinc.cli
    zinc.cli.main()

