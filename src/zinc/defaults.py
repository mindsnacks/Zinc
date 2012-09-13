# -*- coding: utf-8 -*-

"""
zinc.defaults
~~~~~~~~~~~~~

This module provides the Zinc configuration defaults.

"""


#ZINC_FORMAT = "1"

defaults = dict()

defaults['zinc_format'] = '1'
defaults['catalog_index_name'] = 'index.json'
defaults['catalog_config_name'] = 'config.json'
defaults['lock_timeout'] = 60
defaults['gzip_threshhold'] = 0.85
defaults['storage_backend'] = 'filesystem'
