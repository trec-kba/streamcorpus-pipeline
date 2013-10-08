import shutil
import tempfile

import pytest

from ._taggers import name_align_labels, line_offset_align_labels, byte_offset_align_labels
from ._test_data import get_john_smith_tagged_by_lingpipe_path


def _test_config():
    return {
        'tagger_id': 'lingpipe',
        'annotator_id': '',
        'chain_selector': 'ALL',
    }


def test_name_align_labels():
    config = _test_config()
    transform = name_align_labels(config)
    _test_tagger_transform(transform)


def test_line_offset_align_labels():
    config = _test_config()
    transform = line_offset_align_labels(config)
    _test_tagger_transform(transform)


def test_byte_offset_align_labels():
    config = _test_config()
    transform = byte_offset_align_labels(config)
    _test_tagger_transform(transform)


def _test_tagger_transform(transform):
    path = get_john_smith_tagged_by_lingpipe_path()
    with tempfile.NamedTemporaryFile(suffix='.sc') as tf:
        shutil.copyfile(path, tf.name)
        transform.process_path(tf.name)
        # TODO: check some properties of resulting file
