import os
import tarfile
import tempfile

import zinc.utils as utils
import zinc.helpers as helpers

from zinc.models import ZincManifest
from zinc.formats import Formats


def build_archive_with_manifest(manifest: ZincManifest, src_dir, dst_path, flavor=None):

    files = manifest.get_all_files(flavor=flavor)

    with tarfile.open(dst_path, 'w') as tar:
        for f in files:
            format, format_info = manifest.get_format_info_for_file(f)
            assert format is not None
            assert format_info is not None
            sha = manifest.sha_for_file(f)
            ext = helpers.file_extension_for_format(format)

            path = os.path.join(src_dir, f)
            arcname = helpers.append_file_extension(sha, ext)

            # TODO: write a test to ensure that file formats are written correctly

            if format == Formats.RAW:
                tar.add(path, arcname=arcname)

            elif format == Formats.GZ:
                gz_path = tempfile.mkstemp()[1]
                utils.gzip_path(path, gz_path)
                tar.add(gz_path, arcname=arcname)
                os.remove(gz_path)
