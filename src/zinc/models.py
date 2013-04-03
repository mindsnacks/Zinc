import json
import UserDict
from functools import wraps

from zinc.defaults import defaults
from zinc.pathfilter import PathFilter


def mutable_only(f):
    @wraps(f)
    def func(self, *args, **kwargs):
        if not self.is_mutable:
            raise TypeError("Can't modify immutable instance")
        return f(self, *args, **kwargs)
    return func


class ZincModel(object):

    def __init__(self, mutable=True):
        self._mutable = mutable

    @property
    def is_mutable(self):
        return self._mutable

    def to_bytes(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, d, mutable=True):
        raise NotImplementedError()

    @classmethod
    def from_bytes(cls, b, mutable=True):
        d = json.loads(b)
        return cls.from_dict(d, mutable=mutable)

    @classmethod
    def from_path(cls, p, mutable=True):
        with open(p, 'r') as f:
            return cls.from_bytes(f.read(), mutable=mutable)

    def write(self, path):
        with open(path, 'w') as f:
            f.write(self.to_bytes())

    def clone(self, mutable=True):
        d = self.to_dict()
        o = self.__class__.from_dict(d, mutable=mutable)
        return o


### ZincIndex ################################################################

class ZincIndex(ZincModel):

    def __init__(self, id=None, **kwargs):
        super(ZincIndex, self).__init__(**kwargs)
        self._format = defaults['zinc_format']
        self._id = id
        self._bundle_info_by_name = dict()

    def to_dict(self):
        if self.id is None:
            raise ValueError("catalog id is None")  # TODO: better exception?
        return {
            'id': self.id,
            'bundles': self._bundle_info_by_name,
            'format': self._format,
        }

    @property
    def id(self):
        return self._id

    @property
    def format(self):
        return self._format

    @classmethod
    def from_dict(cls, d, mutable=True):
        index = cls(id=d['id'], mutable=mutable)
        index._format = d['format']
        index._bundle_info_by_name = d['bundles']
        return index

    def _get_bundle_info(self, bundle_name):
        info = self._bundle_info_by_name.get(bundle_name)
        if info is not None and self.is_mutable:
            if info.get('next-version'):  # clean mispelled 'next_version' key
                del info['next-version']
        return info

    @mutable_only
    def _get_or_create_bundle_info(self, bundle_name):
        info = self._get_bundle_info(bundle_name)
        if info is None:
            info = self._bundle_info_by_name[bundle_name] = {
                'versions': [],
                'distributions': {},
                'next_version': 0,
            }
        return info

    @mutable_only
    def add_version_for_bundle(self, bundle_name, version):
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        if version not in bundle_info['versions']:
            bundle_info['versions'].append(version)
            bundle_info['versions'] = sorted(bundle_info['versions'])
        else:
            raise ValueError('Bundle version %d already exists.' % (version))

    @mutable_only
    def increment_next_version_for_bundle(self, bundle_name):
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        bundle_info['next_version'] = self.next_version_for_bundle(bundle_name) + 1

    def versions_for_bundle(self, bundle_name):
        info = self._get_bundle_info(bundle_name)
        return info.get('versions') if info is not None else list()

    def next_version_for_bundle(self, bundle_name):
        bundle_info = self._get_bundle_info(bundle_name)
        next_version = bundle_info.get('next_version') if bundle_info else None
        if next_version is None:  # older index without next_version
            versions = self.versions_for_bundle(bundle_name)
            if len(versions) == 0:
                next_version = 0
            else:
                next_version = versions[-1] + 1
            if bundle_info and self.is_mutable:
                bundle_info['next_version'] = next_version
        return next_version

    @mutable_only
    def delete_bundle_version(self, bundle_name, bundle_version):
        assert bundle_version == int(bundle_version)
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        if bundle_info is None:
            raise Exception("Unknown bundle %s" % (bundle_name))
        for distro_name, distro_version in bundle_info['distributions'].iteritems():
            if distro_version == bundle_version:
                raise Exception("bundle '%s' v%d is referenced by the distribution '%s'"
                                % (bundle_name, bundle_version, distro_name))
        versions = bundle_info['versions']
        if bundle_version in versions:
            versions.remove(bundle_version)
        if len(versions) == 0:  # remove info if no more versions
            del self._bundle_info_by_name[bundle_name]
        else:
            bundle_info['versions'] = versions

    def distributions_for_bundle(self, bundle_name):
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        if bundle_info is None:
            raise ValueError("Unknown bundle %s" % (bundle_name))
        return bundle_info['distributions']

    def distributions_for_bundle_by_version(self, bundle_name):
        distros = self.distributions_for_bundle(bundle_name)
        distros_by_version = dict()
        for distro, version in distros.iteritems():
            if distros_by_version.get(version) is None:
                distros_by_version[version] = list()
            distros_by_version[version].append(distro)
        return distros_by_version

    def bundle_names(self):
        return self._bundle_info_by_name.keys()

    def version_for_bundle(self, bundle_name, distro):
        info = self.distributions_for_bundle(bundle_name)
        return info.get(distro) if info else None

    @mutable_only
    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        if int(bundle_version) not in self.versions_for_bundle(bundle_name):
            raise ValueError("Invalid bundle version")
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        bundle_info['distributions'][distribution_name] = bundle_version

    @mutable_only
    def delete_distribution(self, distribution_name, bundle_name):
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        if bundle_name is None:
            raise ValueError("Unknown bundle %s" % (bundle_name))
        del bundle_info['distributions'][distribution_name]


### ZincFileList #############################################################

class ZincFileList(ZincModel, UserDict.DictMixin):

    def __init__(self, **kwargs):
        super(ZincFileList, self).__init__(**kwargs)
        self._files = dict()

    @classmethod
    def from_dict(cls, d, mutable=True):
        obj = cls(mutable=mutable)
        obj._files = d
        return obj

    def __getitem__(self, key):
        return self._files[key]

    @mutable_only
    def __setitem__(self, key, item):
        self._files[key] = item

    @mutable_only
    def __delitem__(self, key):
        del self._files[key]

    def keys(self):
        return self._files.keys()

    def to_dict(self):
        return self._files

    @mutable_only
    def add_file(self, path, sha):
        self._files[path] = {'sha': sha}

    def sha_for_file(self, path):
        return self._files.get(path).get('sha')

    @mutable_only
    def add_flavor_for_file(self, path, flavor):
        props = self._files[path]
        flavors = props.get('flavors') or []
        if not flavor in flavors:
            flavors.append(flavor)
        props['flavors'] = flavors

    def flavors_for_file(self, path):
        return self._files[path].get('flavors')

    @mutable_only
    def add_format_for_file(self, path, format, size):
        props = self._files[path]
        formats = props.get('formats') or {}
        formats[format] = {'size': size}
        props['formats'] = formats

    def formats_for_file(self, path):
        props = self._files[path]
        formats = props.get('formats')
        return formats

    def get_format_info_for_file(self, path, preferred_formats=None):

        if preferred_formats is None:
            preferred_formats = defaults['catalog_preferred_formats']
        for format in preferred_formats:
            format_info = self.formats_for_file(path).get(format)
            if format_info is not None:
                return (format, format_info)

        return (None, None)


    #TODO: naming could be better
    def get_all_files(self, flavor=None):
        all_files = self._files.keys()
        if flavor is None:
            return all_files
        else:
            return [f for f in all_files if flavor in self.flavors_for_file(f)]

    def contents_are_equalivalent(self, other_filelist):
        """
        Checks if the *contents* of two FileLists are equivalent. This checks
        the file paths, sha, and flavors, but skips things like formats. This is
        a looser equality than what `__eq__` provides.
        """
        if len(self) != len(other_filelist):
            return False

        for path in other_filelist.keys():
            my_sha = self.sha_for_file(path)
            if my_sha is None:
                return False

            other_sha = other_filelist.sha_for_file(path)
            if other_sha != my_sha:
                return False

            my_flavors = self.flavors_for_file(path)
            other_flavors = other_filelist.flavors_for_file(path)
            if other_flavors != my_flavors:
                return False

        return True


### ZincManifest #############################################################

class ZincManifest(ZincModel):

    def __init__(self, catalog_id, bundle_name, version, **kwargs):
        super(ZincManifest, self).__init__(**kwargs)
        self._catalog_id = catalog_id
        self._bundle_name = bundle_name
        self._version = int(version)
        self._flavors = []
        self._files = ZincFileList()

    @classmethod
    def from_dict(cls, d, mutable=True):
        catalog_id = d['catalog']
        bundle_name = d['bundle']
        version = int(d['version'])
        manifest = ZincManifest(catalog_id, bundle_name,
                                version, mutable=mutable)
        manifest._files = ZincFileList.from_dict(d['files'])
        manifest._flavors = d.get('flavors') or []  # to support legacy
        return manifest

    @property
    def catalog_id(self):
        return self._catalog_id

    @property
    def version(self):
        return self._version

    @property
    def bundle_name(self):
        return self._bundle_name

    @property
    def files(self):
        return self._files

    @files.setter
    @mutable_only
    def files(self, val):
        if isinstance(val, dict):
            val = ZincFileList.from_dict(val)
        self._files = val
        self._determine_flavors_from_files()

    @property
    def flavors(self):
        return self._flavors

    def _determine_flavors_from_files(self):
        self._flavors = []
        for path, info in self._files.items():
            for flavor in info.get('flavors', []):
                if flavor not in self._flavors:
                    self._flavors.append(flavor)

    @mutable_only
    def add_file(self, path, sha):
        self._files.add_file(path, sha)

    def sha_for_file(self, path):
        return self._files.sha_for_file(path)

    @mutable_only
    def add_flavor_for_file(self, path, flavor):
        self._files.add_flavor_for_file(path, flavor)
        if flavor not in self._flavors:
            self._flavors.append(flavor)

    def flavors_for_file(self, path):
        return self._files.flavors_for_file(path)

    @mutable_only
    def add_format_for_file(self, path, format, size):
        self._files.add_format_for_file(path, format, size)

    def formats_for_file(self, path):
        return self._files.formats_for_file(path)

    def get_format_info_for_file(self, path, preferred_formats=None):
        print path
        return self._files.get_format_info_for_file(path, preferred_formats=preferred_formats)

    #TODO: naming could be better
    def get_all_files(self, flavor=None):
        return self._files.get_all_files(flavor=flavor)

    def to_dict(self):
        return {
            'catalog': self._catalog_id,
            'bundle': self._bundle_name,
            'version': self._version,
            'flavors': self._flavors,
            'files': self._files.to_dict(),
        }

    def __eq__(self, other):
        return self._version == other.version \
                and self._catalog_id == other.catalog_id \
                and self._bundle_name == other.bundle_name \
                and self.files == other.files \
                and set(self.flavors) == set(other.flavors)


### ZincFlavorSpec ############################################################

class ZincFlavorSpec(ZincModel):

    def __init__(self, **kwargs):
        super(ZincFlavorSpec, self).__init__(**kwargs)
        self._filters_by_name = dict()
        self._created_unified_bundle = True

    @property
    def flavors(self):
        return self._filters_by_name.keys()

    def filter_for_flavor(self, flavor_name):
        return self._filters_by_name.get(flavor_name)

    @mutable_only
    def add_flavor(self, flavor_name, path_filter):
        self._filters_by_name[flavor_name] = path_filter

    @classmethod
    def from_dict(cls, d, mutable=True):
        spec = cls(mutable=mutable)
        for k, v in d.iteritems():
            pf = PathFilter.from_rule_list(v)
            spec.add_flavor(k, pf)
        return spec


### ZincCatalogConfig ###############################################################

class ZincCatalogConfig(ZincModel):

    def __init__(self, **kwargs):
        super(ZincCatalogConfig, self).__init__(**kwargs)
        self.gzip_threshhold = 0.85

    @classmethod
    def from_dict(cls, d, mutable=True):
        config = ZincCatalogConfig()
        if d.get('gzip_threshhold'):
            config.gzip_threshhold = d.get('gzip_threshhold')

    def to_dict(self):
        return {
            'gzip_threshold': self.gzip_threshhold
        }
