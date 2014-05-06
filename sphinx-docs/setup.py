#!/usr/bin/env python
from __future__ import absolute_import
from setuptools import setup
from distutils.command.install_data import install_data
from version import get_git_version

import os

VERSION, SOURCE_HASH = get_git_version()
PROJECT = 'diffeo-sphinx'
URL = 'http://diffeo.com'
AUTHOR = 'Diffeo, Inc.'
AUTHOR_EMAIL = 'support@diffeo.com'
DESC = 'Master Diffeo documentation set.'
LICENSE = 'Diffeo Proprietary Commercial Computer Software'

def subtree(t, d):
    for dirpath, dirnames, filenames in os.walk(d):
        yield (os.path.join(t, os.path.relpath(dirpath, d)),
               [os.path.join(dirpath, filename) for filename in filenames])

class install_data_sphinx(install_data):
    def run(self):
        self.run_command('build_sphinx')
        self.data_files.remove('MARKER.txt')
        sphinx = self.get_finalized_command('build_sphinx')
        self.data_files += list(subtree('docs/html',
                                        os.path.join(sphinx.build_dir, 'html')))
        install_data.run(self)

setup(
    name=PROJECT,
    version=VERSION,
    #source_label=SOURCE_HASH,
    license=LICENSE,
    description=DESC,
    #long_description=read_file('README.rst'),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    cmdclass={'install_data': install_data_sphinx},

    install_requires=[
        'docutils',
        'Sphinx',
        'sphinxcontrib-httpdomain',

        # We list these for completeness.  However, note that because
        # of the way pip runs, 'pip install diffeo-sphinx' on a totally
        # clean virtualenv will produce broken documentation; it will
        # install the following dependency packages, then install this
        # (running Sphinx), then install recursive dependencies.
        'bigforest',
        'dblogger',
        'diffeo-cyber',
        'diffeo-spinn3r',
        'kvlayer',
        'rejester',
        'streamcorpus-pipeline',
        'streamcorpus',
        'treelab',
        'yakonfig',
    ],
    # there must be a data_files for install_data to run
    data_files=['MARKER.txt'],
)
