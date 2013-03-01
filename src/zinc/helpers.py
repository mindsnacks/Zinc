
### Utils

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

def bundle_id_for_catalog_id_and_bundle_name(catalog_id, bundle_name):
    return '%s.%s' % (catalog_id, bundle_name)

def bundle_descriptor_for_bundle_id_and_version(bundle_id, version, flavor=None):
    descriptor = '%s-%d' % (bundle_id, version)
    if flavor is not None: descriptor += '~%s' % (flavor)
    return descriptor

