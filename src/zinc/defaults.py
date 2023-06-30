# -*- coding: utf-8 -*-

"""
zinc.defaults
~~~~~~~~~~~~~

This module provides the Zinc configuration defaults.

Configurations:
:zinc_format: The format version of the Zinc catalog. Current only '1' is supported.
:catalog_index_name: The name of the catalog index file.
:catalog_index_max_age_seconds: The maximum length of time for which a catalog index may be cached. It is the responsibility of the storage backend to handle.  Some backends (such as the 'FileSystemStorageBackend') may ignore this setting.
:catalog_write_legacy_index: Specify that the legacy 'index.json' should be written in addition to the current catalog index file.
:catalog_config_name: The name of the catalog index file (currently unused).
:catalog_preferred_formats: An ordered list of the formats to try when locating a file. Must be a (non-strict) subset of 'catalog_valid_formats'.
:catalog_valid_formats: A list of valid formats for objects in the catalog.
:catalog_lock_timeout: Timeout for acquiring a lock on the catalog via a coordinator.
:catalog_prev_distro_prefix: The prefix to use when writing the previous distro.
"""

from .formats import Formats

defaults = dict()


defaults['zinc_format'] = '1'
defaults['catalog_index_name'] = 'catalog.json'
defaults['catalog_index_max_age_seconds'] = 300
defaults['catalog_write_legacy_index'] = True  # TODO: move this to config once config is implemented
defaults['catalog_config_name'] = 'config.json'
defaults['catalog_preferred_formats'] = [Formats.GZ, Formats.RAW]
defaults['catalog_valid_formats'] = defaults['catalog_preferred_formats']
defaults['catalog_lock_timeout'] = 60
defaults['catalog_prev_distro_prefix'] = '_'
