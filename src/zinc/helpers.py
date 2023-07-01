
# Utils

from .formats import Formats
from .defaults import defaults

from typing import List, Optional


def make_bundle_id(catalog_id: str, bundle_name: str) -> str:
    assert catalog_id
    assert bundle_name
    return '%s.%s' % (catalog_id, bundle_name)


def make_bundle_descriptor(bundle_id: str, version: int, flavor: Optional[str] = None) -> str:
    assert bundle_id
    assert version
    descriptor = '%s-%d' % (bundle_id, version)
    if flavor is not None:
        descriptor += '~%s' % (flavor)
    return descriptor


def _bundle_descriptor_without_flavor(bundle_descriptor: str) -> str:
    index = bundle_descriptor.rfind('~')
    if index == -1:
        return bundle_descriptor
    else:
        return bundle_descriptor[:index]


def bundle_id_from_bundle_descriptor(bundle_descriptor: str) -> str:
    bundle_desc_without_flavor = _bundle_descriptor_without_flavor(bundle_descriptor)
    return bundle_desc_without_flavor[:bundle_desc_without_flavor.rfind('-')]


def bundle_version_from_bundle_descriptor(bundle_descriptor: str) -> int:
    bundle_desc_without_flavor = _bundle_descriptor_without_flavor(bundle_descriptor)
    version_flavor = bundle_desc_without_flavor[bundle_desc_without_flavor.rfind('-') + 1:]
    version = int(version_flavor.split('~')[0])
    return version


# TODO: move to formats.py?
def file_extension_for_format(format: Formats) -> Optional[str]:
    """Returns proper file extension for the given format, or `None` if no
    extension is necessary."""

    if format == Formats.RAW:
        return None
    return format


def append_file_extension(path: str, ext: Optional[str]) -> str:
    """Appends the given extension to the path. If `ext` is `None`,
    `path` is returned."""

    if ext is not None:
        return '%s.%s' % (path, ext)
    return path


def append_file_extension_for_format(path: str, format: Formats) -> str:
    """Appends the proper filename extension for the given format."""

    ext = file_extension_for_format(format)
    return append_file_extension(path, ext)


def distro_previous_name(distro_name: str) -> str:
    return '%s%s' % (defaults['catalog_prev_distro_prefix'], distro_name)


def distro_name_errors(distro_name) -> List[str]:
    """Returns a list of errors in the distro name. Empty list means distro name
    is valid."""

    errors = []

    if len(distro_name) == 0:
        errors.append(
            "distro name cannot be zero length")

    if distro_name.startswith(defaults['catalog_prev_distro_prefix']):
        errors.append(
            "%s is a reserved prefix" % (defaults['catalog_prev_distro_prefix']))

    return errors
