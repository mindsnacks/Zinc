import json
import UserDict

from zinc.utils import *
from zinc.defaults import defaults
from zinc.pathfilter import PathFilter

class ZincModel(object):

    def to_bytes(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_bytes(cls, b):
        d = json.loads(b)
        return cls.from_dict(d)

    @classmethod
    def from_path(cls, p):
        with open(p, 'r') as f:
            return cls.from_bytes(f.read())

    def write(self, path):
        with open(path, 'w') as f:
            f.write(self.to_bytes())


### ZincIndex ################################################################

class ZincIndex(ZincModel):

    def __init__(self, id=None):
        self._format = defaults['zinc_format']
        self._id = id
        self._bundle_info_by_name = dict()

    def to_dict(self):
        if self.id is None:
            raise ValueError("catalog id is None") # TODO: better exception?
        return {
               'id' : self.id,
                'bundles' : self._bundle_info_by_name,
                'format' : self._format,
                }

    @property
    def id(self):
        return self._id

    @property
    def format(self):
        return self._format

    @classmethod
    def from_dict(cls, d):
        index = cls()
        index._id = d['id']
        index._format = d['format']
        index._bundle_info_by_name = d['bundles']
        return index

    def _get_or_create_bundle_info(self, bundle_name):
        if self._bundle_info_by_name.get(bundle_name) is None:
            self._bundle_info_by_name[bundle_name] = {
                    'versions':[],
                    'distributions':{},
                    'next_version':1,
                    }
        return self._bundle_info_by_name.get(bundle_name)

    def add_version_for_bundle(self, bundle_name, version):
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        if version not in bundle_info['versions']:
            next_version = self.next_version_for_bundle(bundle_name) 
            if version != next_version:
                raise Exception("Expected next bundle version %d, got version %d" 
                        % (verison, next_version))
            bundle_info['versions'].append(version)
            bundle_info['versions'] = sorted(bundle_info['versions'])
            bundle_info['next_version'] = version + 1

    def versions_for_bundle(self, bundle_name):
        return self._get_or_create_bundle_info(bundle_name).get('versions')

    def next_version_for_bundle(self, bundle_name):
        bundle_info = self._get_or_create_bundle_info(bundle_name)

        next_version = bundle_info.get('next_version')
        if next_version is None: # older index without next_version
            versions = self.versions_for_bundle(bundle_name)
            if len(versions) == 0:
                next_version = 1
            else: 
                next_version = versions[-1] + 1
            bundle_info['next-version'] = next_version

        return next_version
       
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
        if len(versions) == 0: # remove info if no more versions
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
            if distros_by_version.get(version) == None:
                distros_by_version[version] = list()
            distros_by_version[version].append(distro)
        return distros_by_version

    def bundle_names(self):
        return self._bundle_info_by_name.keys()

    def version_for_bundle(self, bundle_name, distro):
        return self.distributions_for_bundle(bundle_name).get(distro)

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        if int(bundle_version) not in self.versions_for_bundle(bundle_name):
            raise ValueError("Invalid bundle version")
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        bundle_info['distributions'][distribution_name] = bundle_version

    def delete_distribution(self, distribution_name, bundle_name):
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        if bundle_name is None:
            raise ValueError("Unknown bundle %s" % (bundle_name))
        del bundle_info['distributions'][distribution_name]


### ZincFileList #############################################################

class ZincFileList(ZincModel, UserDict.DictMixin):

    def __init__(self):
        self._files = dict()
    
    @classmethod
    def from_dict(cls, d):
        obj = cls()
        obj._files = d
        return obj

    def __getitem__(self, key):
        return self._files[key]

    def __setitem__(self, key, item):
        self._files[key] = item

    def __delitem__(self, key):
        del self._files[key]

    def keys(self):
        return self._files.keys()
    
    def to_dict(self):
        return self._files

    def add_file(self, path, sha):
        self._files[path] = {'sha' : sha}

    def sha_for_file(self, path):
        return self._files.get(path).get('sha')

    def add_format_for_file(self, path, format, size):
        props = self._files[path]
        formats = props.get('formats') or {}
        formats[format] = {'size' : size}
        props['formats'] = formats

    def formats_for_file(self, path):
        props = self._files[path]
        formats = props.get('formats')
        return formats

    def get_format_info_for_file(self, path, preferred_formats=None):

        if preferred_formats is None:
            preferred_formats = ['gz', 'raw']

        for format in preferred_formats:
            format_info = self.formats_for_file(path).get(format)
            if format_info is not None:
                return (format, format_info)

        return (None, None)

    def add_flavor_for_file(self, path, flavor):
        props = self._files[path]
        flavors = props.get('flavors') or []
        if not flavor in flavors:
            flavors.append(flavor)
        props['flavors'] = flavors

    def flavors_for_file(self, path):
        return self._files[path].get('flavors')

    #TODO: naming could be better
    def get_all_files(self, flavor=None):
        all_files = self._files.keys()
        if flavor is None:
            return all_files
        else:
            return [f for f in all_files if flavor in self.flavors_for_file(f)]


### ZincManifest #############################################################

class ZincManifest(ZincModel):

    def __init__(self, catalog_id, bundle_name, version=1):
        self._catalog_id = catalog_id
        self._bundle_name = bundle_name
        self._version = int(version)
        self._flavors = []
        self._files = ZincFileList()

    @classmethod
    def from_dict(cls, d):
        catalog_id = d['catalog']
        bundle_name = d['bundle']
        version = int(d['version'])
        manifest = ZincManifest(catalog_id, bundle_name, version)
        manifest._files = ZincFileList.from_dict(d['files'])
        manifest._flavors = d.get('flavors') or [] # to support legacy
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

    def add_file(self, path, sha):
        self._files.add_file(path, sha)

    def sha_for_file(self, path):
        return self._files.sha_for_file(path)

    def add_format_for_file(self, path, format, size):
        self._files.add_format_for_file(path, format, size)

    def formats_for_file(self, path):
        return self._files.formats_for_file(path)
        
    def get_format_info_for_file(self, path, preferred_formats=None):
        return self._files.get_format_info_for_file(
                path, preferred_formats=preferred_formats)

    def add_flavor_for_file(self, path, flavor):
        self._files.add_flavor_for_file(path, flavor)
        if flavor not in self._flavors:
            self._flavors.append(flavor)

    def flavors_for_file(self, path):
        return self._files.flavors_for_file(path)

    #TODO: naming could be better
    def get_all_files(self, flavor=None):
        return self._files.get_all_files(flavor=flavor)

    def to_dict(self):
        return {
                'catalog' : self._catalog_id,
                'bundle' : self._bundle_name,
                'version' : self._version,
                'flavors' : self._flavors,
                'files' : self._files.to_dict(),
                }

    def files_are_equivalent(self, other):
        # check that the keys are all the same
        if len(set(self._files.keys()) - set(other._files.keys())) != 0:
            return False
        if len(set(other._files.keys()) - set(self._files.keys())) != 0:
            return False
        # if the keys are all the same, check the values
        for (file, props) in self._files.items():
            sha = props.get('sha')
            other_sha = other._files.get(file).get('sha')
            if other_sha is None or sha != other_sha:
                return False
        return True

    def __eq__(self, other):
        return self._version == other.version \
                and self._catalog_id == other.catalog_id \
                and self._bundle_name == other.bundle_name \
                and self.files == other.files \
                and set(self.flavors) == set(other.flavors)


### ZincFlavorSpec ############################################################

class ZincFlavorSpec(ZincModel):

    def __init__(self):
        self._filters_by_name = dict()
        self._created_unified_bundle = True

    def add_flavor(self, flavor_name, path_filter):
        self._filters_by_name[flavor_name] = path_filter

    @property
    def flavors(self):
        return self._filters_by_name.keys()

    def filter_for_flavor(self, flavor_name):
        return self._filters_by_name.get(flavor_name)

    @classmethod
    def from_dict(cls, d):
        spec = ZincFlavorSpec()
        for k, v in d.iteritems():
            pf = PathFilter.from_rule_list(v)
            spec.add_flavor(k, pf)
        return spec

