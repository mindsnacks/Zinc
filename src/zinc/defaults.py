# -*- coding: utf-8 -*-

"""
zinc.defaults
~~~~~~~~~~~~~

This module provides the Zinc configuration defaults.

"""

defaults = dict()
defaults['zinc_format'] = '1'
defaults['catalog_index_name'] = 'index.json'
defaults['catalog_config_name'] = 'config.json'
defaults['catalog_preferred_formats'] = ['gz', 'raw']
defaults['catalog_valid_formats'] = defaults['catalog_preferred_formats']
defaults['catalog_lock_timeout'] = 60
