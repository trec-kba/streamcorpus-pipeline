'''py.test hooks for streamcorpus-pipeline.'''
from __future__ import absolute_import
import os
import sys
import sysconfig

import pytest


@pytest.fixture
def test_data_dir(request):
    # Determine directory where this file lives and return
    # different paths depending on whether or not this
    # conftest.py file has been installed
    cur_directory_path = os.path.abspath(os.path.dirname(__file__))

    if cur_directory_path.startswith(os.path.abspath(sys.prefix)):
        # Running from an installed location
        path = os.path.join(sysconfig.get_path('data'),
                            'data/streamcorpus-pipeline')
    else:
        # Running not installed
        path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '../../data'))
    return path
