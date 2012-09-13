import os
from os.path import join as pjoin
import json
import logging
import functools
from contextlib import contextmanager

from zinc.utils import sha1_for_path, makedirs, gzip_path
from zinc.defaults import defaults
from zinc.pathfilter import PathFilter

import uuid


### Errors ###################################################################

class ZincError(object):
    def __init__(self, code, message):
        self.code = code
        self.message = message

# TODO: might be worst python ever
class ZincErrors(object):
    OK = ZincError(0, "OK")
    INCORRECT_SHA = ZincError(1, "SHA did not match")
    DOES_NOT_EXIST = ZincError(2, "Does not exist")
    INVALID_VERSION = ZincError(3, "Invalid version")

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

### ZincIndex ################################################################


class ZincIndex(object):

    #def bundle_locking(func_to_decorate):
    #    @functools.wraps(func_to_decorate)
    #    def wrapper(*args, **kwargs):
    #        self = args[0]
    #        bundle_name = args[1]
    #        bundle_lock = self._lock_bundle(bundle_name)
    #        result = func_to_decorate(*args, **kwargs)
    #        self._unlock_bundle(bundle_name, bundle_lock)
    #        self._save()
    #        return result
    #    return wrapper

    @contextmanager
    def bundle_lock(self, bundle_name, require_exists=True):
        bundle_lock = self._lock_bundle(bundle_name,
                require_exists=require_exists)
        yield
        self._unlock_bundle(bundle_name, bundle_lock)
        self._save()

    def __init__(self, id=None):
        self.id = id

    def versions_for_bundle(self, bundle_name):
        with self.bundle_lock(bundle_name, require_exists=False):
            return self._versions_for_bundle(bundle_name)

    def add_version_for_bundle(self, bundle_name):
        with self.bundle_lock(bundle_name, require_exists=False):
            return self._add_version_for_bundle(bundle_name)

    def delete_bundle_version(self, bundle_name, version):
        if version != int(version):
            raise ValueError("Invalid version %s", version)

        with self.bundle_lock(bundle_name, require_exists=False):
            distros = self._distributions_for_bundle(bundle_name)
            for distro in distros:
                distro_version = self._bundle_version_for_distro(distro)
                if distro_version == bundle_version:
                     raise Exception("bundle '%s' v%d is referenced by the distribution '%s'" 
                            % (bundle_name, version, distro))

            return self._delete_bundle_version(bundle_name, version)
    
    def distributions_for_bundle(self, bundle_name):
        with self.bundle_lock(bundle_name):
            return self._distributions_for_bundle(bundle_name)

    def update_distribution(self, bundle_name, version, distro_name):
        if version != int(version):
            raise ValueError("Invalid version %s", version)

        with self.bundle_lock(bundle_name):
            if version == 'latest':
                version = self._versions_for_bundle(bundle_name)[-1]

            if int(version) not in self._versions_for_bundle(bundle_name):
                # TODO: Not sure a raise is the right thing to do here.
                # Might screw up the lock context
                raise LookupError("Invalid bundle version")
            else:
                return self._update_distribution(bundle_name, version, distro_name)

    def delete_distribution(self, bundle_name, distro_name):
        with self.bundle_lock(bundle_name):
            return self._delete_distribution(bundle_name, distro_name)

    def bundle_version_for_distro(self, bundle_name, distro):
        with self.bundle_lock(bundle_name):
            return self._bundle_version_for_distro(bundle_name, distro)

    def _bundle_version_for_distro(self, bundle_name, distro):
        return self._distributions_for_bundle(bundle_name).get(distro)

    def distributions_for_bundle_by_version(self, bundle_name):
        distros = self.distributions_for_bundle(bundle_name)
        distros_by_version = dict()
        for distro, version in distros.iteritems():
            if distros_by_version.get(version) == None:
                distros_by_version[version] = list()
            distros_by_version[version].append(distro)
        return distros_by_version

   # For Subclasses
        
    def _bundle_exists(self, bundle_name):
        raise NotImplementedError

    def _lock_bundle(self, bundle_name, timeout=None, require_exists=True):
        raise NotImplementedError

    def _unlock_bundle(self, bundle_name, lock):
        raise NotImplementedError

    def _add_version_for_bundle(self, bundle_name):
        raise NotImplementedError

    def _delete_bundle_version(self, bundle_name, version):
        raise NotImplementedError

    def _distributions_for_bundle(self, bundle_name):
        raise NotImplementedError

    def _update_distribution(self, bundle_name, version, distro_name):
        raise NotImplementedError

    def _delete_distribution(self, bundle_name, distro_name):
        raise NotImplementedError

    def _save(self):
        raise NotImplementedError

    def to_dict(self, format=None):
        raise NotImplementedError

    # Internal Utility Methods

    def _next_version_for_bundle(self, bundle_name):
        versions = self._versions_for_bundle(bundle_name)
        if len(versions) == 0:
            return 1
        return versions[-1] + 1


class ObjectZincIndex(ZincIndex):

    def __init__(self, id=None, backend=None):
        self.id = id
        self._bundle_info_by_name = dict()

    def to_dict(self, format=None):
        if format is None:
            format = defaults['zinc_format']

        if format == '1':
            return {
		    		'id' : self.id,
                    'bundles' : self._bundle_info_by_name,
                    'format' : format,
                    }
        raise ValueError('Unknown format %s' % (format))

    @classmethod
    def from_dict(cls, json_dict, format=None):
        if format is None:
            format = defaults['zinc_format']
        index = cls()
        index.id = json_dict['id']
        index.format = json_dict['format']
        index._bundle_info_by_name = json_dict['bundles']
        return index

    def _save(self):
        pass

    def _bundle_exists(self, bundle_name):
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        return bundle_info is not None

    def _lock_bundle(self, bundle_name, timeout=None, require_exists=True):
        if require_exists and not self._bundle_exists(bundle_name):
            raise LookupError("Unknown bundle %s" % (bundle_name))
        lock = uuid.uuid4()
        print "lock %s %s" % (bundle_name, lock)
        return lock

    def _unlock_bundle(self, bundle_name, lock):
        print "unlock %s %s" % (bundle_name, lock)

    def _get_or_create_bundle_info(self, bundle_name):
        if self._bundle_info_by_name.get(bundle_name) is None:
            self._bundle_info_by_name[bundle_name] = {
                    'versions':[],
                    'distributions':{},
                    }
        return self._bundle_info_by_name.get(bundle_name)

    def _add_version_for_bundle(self, bundle_name):
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        version = self._next_version_for_bundle(bundle_name)
        if version not in bundle_info['versions']:
            bundle_info['versions'].append(version)
            bundle_info['versions'] = sorted(bundle_info['versions'])
        return version

    def _versions_for_bundle(self, bundle_name):
        return self._get_or_create_bundle_info(bundle_name).get('versions')

    def _delete_bundle_version(self, bundle_name, bundle_version):
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        versions = bundle_info['versions']
        if bundle_version in versions:
            versions.remove(bundle_version)
        if len(versions) == 0: # remove info if no more versions
            del self._bundle_info_by_name[bundle_name]
        else:
            bundle_info['versions'] = versions
        
    def _distributions_for_bundle(self, bundle_name):
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        return bundle_info['distributions']

    def _update_distribution(self, bundle_name, bundle_version, distro_name):
        bundle_info = self._get_or_create_bundle_info(bundle_name)
        bundle_info['distributions'][distro_name] = bundle_version

    def _delete_distribution(self, bundle_name, distro_name):
        bundle_info = self._bundle_info_by_name.get(bundle_name)
        del bundle_info['distributions'][distro_name]


### ZincConfig ###############################################################

class ZincConfig(object):

    def __init__(self):
        self.gzip_threshhold = 0.85

    def to_dict(self):
        d = {}
        if self.gzip_threshhold is not None:
            d['gzip_threshhold'] = self.gzip_threshhold
        return d
   
    def write(self, path):
        config_file = open(path, 'w')
        dict = self.to_dict()
        config_file.write(json.dumps(dict))
        config_file.close()

def load_config(path):
    config_file = open(path, 'r')
    dict = json.load(config_file)
    config_file.close()
    config = ZincConfig()
    if dict.get('gzip_threshhold'):
        config.gzip_threshhold = dict.get('gzip_threshhold')
    return config 

### ZincManifest #############################################################

class ZincManifest(object):

    def __init__(self, catalog_id, bundle_name, version=1):
        self.catalog_id = catalog_id
        self.bundle_name = bundle_name
        self.version = int(version)
        self._flavors = []
        self.files = dict()

    def add_file(self, path, sha):
        self.files[path] = {'sha' : sha}

    def sha_for_file(self, path):
        return self.files.get(path).get('sha')

    def add_format_for_file(self, path, format, size):
        props = self.files[path]
        formats = props.get('formats') or {}
        formats[format] = {'size' : size}
        props['formats'] = formats

    def formats_for_file(self, path):
        props = self.files[path]
        formats = props.get('formats')
        return formats
        
    def add_flavor_for_file(self, path, flavor):
        props = self.files[path]
        flavors = props.get('flavors') or []
        if not flavor in flavors:
            flavors.append(flavor)
        props['flavors'] = flavors
        if flavor not in self._flavors:
            self._flavors.append(flavor)

    #TODO: naming could be better
    def get_all_files(self, flavor=None):
        all_files = self.files.keys()
        if flavor is None:
            return all_files
        else:
            return [f for f in all_files if flavor in self.flavors_for_file(f)]

    @property
    def flavors(self):
        return self._flavors

    def flavors_for_file(self, path):
        return self.files[path].get('flavors')

    def to_dict(self):
        return {
                'catalog' : self.catalog_id,
                'bundle' : self.bundle_name,
                'version' : self.version,
                'flavors' : self._flavors,
                'files' : self.files,
                }

    @classmethod
    def from_dict(cls, json_dict):
        catalog_id = json_dict['catalog']
        bundle_name = json_dict['bundle']
        version = int(json_dict['version'])
        manifest = ZincManifest(catalog_id, bundle_name, version)
        manifest.files = json_dict['files']
        manifest._flavors = json_dict.get('flavors') or [] # to support legacy
        return manifest

    def files_are_equivalent(self, other):
        # check that the keys are all the same
        if len(set(self.files.keys()) - set(other.files.keys())) != 0:
            return False
        if len(set(other.files.keys()) - set(self.files.keys())) != 0:
            return False
        # if the keys are all the same, check the values
        for (file, props) in self.files.items():
            sha = props.get('sha')
            other_sha = other.files.get(file).get('sha')
            if other_sha is None or sha != other_sha:
                return False
        return True

    def equals(self, other):
        return self.version == other.version \
                and self.catalog_id == other.catalog_id \
                and self.bundle_name == other.bundle_name \
                and self.files_are_equivalent(other) \
                and set(self.flavors) == set(other.flavors)


##############################################################################


### ZincFlavorSpec ############################################################

class ZincFlavorSpec(object):
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

    @staticmethod
    def from_dict(d):
        spec = ZincFlavorSpec()
        for k, v in d.iteritems():
            pf = PathFilter.from_rule_list(v)
            spec.add_flavor(k, pf)
        return spec


### ZincCatalog ################################################################

class ZincCatalog(object):

    def _load(self):
        self.index = self.index_backend.load_index()
        self._read_config_file()

    def __init__(self, index_backend, storage_backend):

        self.index_backend = index_backend
        self.storage_backend = storage_backend

        self._manifests = {}

        self._load()

    @property
    def id(self):
        return self.index.id

    def format(self):
        return self.index.format

    def _files_dir(self):
        return "objects"

    def _manifests_dir(self):
        return "manifests"

    def _archives_dir(self):
        return "archives"

    def _read_config_file(self):
        # TODO: QQQ
        #config_path = pjoin(self.path, defaults['catalog_config_name'])
        #self.config = load_config(config_path)
        self.config = ZincConfig()

    def _path_for_manifest_for_bundle_version(self, bundle_name, version):
        manifest_filename = "%s-%d.json" % (bundle_name, version)
        manifest_path = pjoin(self._manifests_dir(), manifest_filename)
        return manifest_path

    def _path_for_archive_for_bundle_version(self, bundle_name, version,
            flavor=None):
        archive_filename = self._archive_filename(
                bundle_name, version, flavor=flavor)
        archive_path = pjoin(self._archives_dir(), archive_filename)
        return archive_path

    def _archive_filename(self, bundle_name, version, flavor=None):
        if flavor is None:
            archive_filename = "%s-%d.tar" % (bundle_name, version)
        else:
            archive_filename = "%s-%d~%s.tar" % (bundle_name, version, flavor)
        return archive_filename

    def _path_for_manifest(self, manifest):
        return self._path_for_manifest_for_bundle_version(
                manifest.bundle_name, manifest.version)

    def manifest_for_bundle(self, bundle_name, version=None):
        all_versions = self.index.versions_for_bundle(bundle_name)
        if version is None and len(all_versions) > 0:
            version = all_versions[-1]
        elif version not in all_versions:
            return None # throw exception?
        manifest_path = self._path_for_manifest_for_bundle_version(
                bundle_name, version)
        manifest_dict = self.storage_backend.read_json_dict(manifest_path)
        return ZincManifest.from_dict(manifest_dict)

    def manifest_for_bundle_descriptor(self, bundle_descriptor):
        return self.manifest_for_bundle(
            bundle_id_from_bundle_descriptor(bundle_descriptor),
            bundle_version_from_bundle_descriptor(bundle_descriptor))
            
    def _write_manifest(self, manifest):
        path = self._path_for_manifest(manifest)
        self.storage_backend.write_json_dict(
                manifest.to_dict(), path, gzip=True)

    def _path_for_file_with_sha(self, sha, ext=None):
        subdir = pjoin(self._files_dir(), sha[0:2], sha[2:4])
        file = sha
        if ext is not None:
            file = file + '.' + ext
        return pjoin(subdir, file)

    def _import_path(self, src_path):
        src_path_sha = sha1_for_path(src_path)
        dst_path = self._path_for_file_with_sha(src_path_sha)
        dst_path_gz = dst_path + '.gz'

        # TODO: this is lame
        if os.path.exists(dst_path):
            return (dst_path, os.path.getsize(dst_path))
        elif os.path.exists(dst_path_gz):
            return (dst_path_gz, os.path.getsize(dst_path_gz))

        # gzip the file first, and see if it passes the compression
        # threshhold

        makedirs(os.path.dirname(dst_path))
        gzip_path(src_path, dst_path_gz)
        src_size = os.path.getsize(src_path)
        dst_gz_size = os.path.getsize(dst_path_gz)
        if float(dst_gz_size) / src_size <= self.config.gzip_threshhold:
            final_dst_path = dst_path_gz
            final_dst_size = dst_gz_size
        else:
            final_dst_path = dst_path
            final_dst_size = src_size
            self.storage_backend.write_path(src_path, dst_path)
            os.remove(dst_path_gz)

        logging.info("Imported %s --> %s" % (src_path, final_dst_path))
        return (final_dst_path, final_dst_size)

    def _import_archive(self, src_path, bundle_name, version, flavor=None):

        dst_path = self._path_for_archive_for_bundle_version(
                bundle_name, version, flavor=flavor)
        self.storage_backend.write_path(src_path, dst_path)

    def bundle_descriptors(self):
        bundle_descriptors = []
        for bundle_name in self.bundle_names():
            for version in self.versions_for_bundle(bundle_name):
                bundle_descriptors.append("%s-%d" % (bundle_name, version))
                manifest = self.manifest_for_bundle(bundle_name, version)
                for flavor in manifest.flavors:
                    bundle_descriptors.append("%s-%d~%s" % 
                            (bundle_name, version, flavor))
        return bundle_descriptors

    def clean(self, dry_run=False):
        bundle_descriptors = self.bundle_descriptors()
        verb = 'Would remove' if dry_run else 'Removing'

        ### 1. scan manifests for ones that aren't in index
        for root, dirs, files in os.walk(self._manifests_dir()):
            for f in files:
                remove = False
                if not (f.endswith(".json") or f.endswith(".json.gz")):
                    # remove stray files
                    remove = True
                else:
                    bundle_descr = f.split(".")[0]
                    if bundle_descr not in bundle_descriptors:
                        remove = True
                if remove:
                    logging.info("%s %s" % (verb, pjoin(root, f)))
                    if not dry_run: os.remove(pjoin(root, f))

        ### 2. scan archives for ones that aren't in index
        for root, dirs, files in os.walk(self._archives_dir()):
            for f in files:
                remove = False
                if not (f.endswith(".tar")):
                    # remove stray files
                    remove = True
                else:
                    bundle_descr = f.split(".")[0]
                    if bundle_descr not in bundle_descriptors:
                        remove = True
                if remove:
                    logging.info("%s %s" % (verb, pjoin(root, f)))
                    if not dry_run: os.remove(pjoin(root, f))

        ### 3. clean objects
        all_objects = set()
        for bundle_desc in bundle_descriptors:
            manifest = self.manifest_for_bundle_descriptor(bundle_desc)
            for f, meta in manifest.files.iteritems():
                all_objects.add(meta['sha'])
        for root, dirs, files in os.walk(self._files_dir()):
            for f in files:
                basename = os.path.splitext(f)[0]
                if basename not in all_objects:
                    logging.info("%s %s" % (verb, pjoin(root, f)))
                    if not dry_run: os.remove(pjoin(root, f))

    def add_bundle_version(self, bundle_name):
        self.index.add_version_for_bundle(bundle_name, version)

    #def _add_manifest(self, bundle_name, version=1):
    #    if version in self.versions_for_bundle(bundle_name):
    #        raise ValueError("Bundle already exists")
    #        return None

    #    manifest = ZincManifest(self.index.id, bundle_name, version)
    #    self._write_manifest(manifest)
    #    return manifest

    def versions_for_bundle(self, bundle_name):
        return self.index.versions_for_bundle(bundle_name)

    def bundle_names(self):
        return self.index.bundle_info_by_name.keys()

    def delete_bundle_version(self, bundle_name, version):
        self.index.delete_bundle_version(bundle_name, version)
        self.save()

    def update_distribution(self, distribution_name, bundle_name, bundle_version):
        self.index.update_distribution(distribution_name, bundle_name, bundle_version)
        self.save()

    def delete_distribution(self, distribution_name, bundle_name):
        self.index.delete_distribution(distribution_name, bundle_name)
        self.save()

    def save(self):
        self.index.save()

    def create_bundle_version(self, bundle_name, src_dir, 
            flavor_spec=None, force=False):
        from zinc.tasks.bundle_create import ZincBundleCreateTask
        task = ZincBundleCreateTask(
                self, bundle_name, src_dir, flavor_spec, force)
        return task.run()

    def verify(self):
        for (bundle_name, bundle_info) in self.index.bundle_info_by_name.iteritems():
            for version in bundle_info['versions']:
                manifest = self.manifest_for_bundle(bundle_name, version)
                if manifest is None:
                    raise Exception("manifest not found: %s-%d" % (bundle_name,
                        version))
                #for (file, sha) in manifest.files.iteritems():
                #    print file, sha

        results_by_file = dict()
        #for version, manifest in self.manifests.items():
        #    files = manifest.get("files")
        #    for file, sha in files.items():
        #        full_path = pjoin(self.path, self.path_for_file(file, version))
        #        logging.debug("verifying %s" % full_path)
        #        if not os.path.exists(full_path):
        #            results_by_file[file] = ZincErrors.DOES_NOT_EXIST
        #        elif sha1_for_path(full_path) != sha:
        #            results_by_file[file] = ZincErrors.INCORRECT_SHA
        #        else:
        #            # everything is ok alarm
        #            results_by_file[file] = ZincErrors.OK
        return results_by_file



