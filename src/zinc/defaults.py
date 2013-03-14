# -*- coding: utf-8 -*-

"""
zinc.defaults
~~~~~~~~~~~~~

This module provides the Zinc configuration defaults.

"""

from .formats import Formats

defaults = dict()
defaults['zinc_format'] = '1'
defaults['catalog_index_name'] = 'index.json'
defaults['catalog_config_name'] = 'config.json'
defaults['catalog_preferred_formats'] = [Formats.GZ, Formats.RAW]
defaults['catalog_valid_formats'] = defaults['catalog_preferred_formats']
defaults['catalog_lock_timeout'] = 60
