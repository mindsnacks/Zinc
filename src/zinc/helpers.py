
### Utils

from .formats import Formats
from .defaults import defaults


def make_bundle_id(catalog_id, bundle_name):
    assert catalog_id
    assert bundle_name
    return '%s.%s' % (catalog_id, bundle_name)


def make_bundle_descriptor(bundle_id, version, flavor=None):
    assert bundle_id
    assert version
    descriptor = '%s-%d' % (bundle_id, version)
    if flavor is not None:
        descriptor += '~%s' % (flavor)
    return descriptor


def _bundle_descriptor_without_flavor(bundle_descriptor):
    index = bundle_descriptor.rfind('~')
    if index == -1:
        return bundle_descriptor
    else:
        return bundle_descriptor[:index]


def bundle_id_from_bundle_descriptor(bundle_descriptor):
    bundle_desc_without_flavor = _bundle_descriptor_without_flavor(bundle_descriptor)
    return bundle_desc_without_flavor[:bundle_desc_without_flavor.rfind('-')]


def bundle_version_from_bundle_descriptor(bundle_descriptor):
    bundle_desc_without_flavor = _bundle_descriptor_without_flavor(bundle_descriptor)
    version_flavor = bundle_desc_without_flavor[bundle_desc_without_flavor.rfind('-') + 1:]
    version = int(version_flavor.split('~')[0])
    return version


# TODO: move to formats.py?
def file_extension_for_format(format):
    if format == Formats.RAW:
        return None
    return format


def distro_previous_name(distro_name):
    return '%s%s' % (defaults['catalog_prev_distro_prefix'], distro_name)


def distro_name_errors(distro_name):
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
