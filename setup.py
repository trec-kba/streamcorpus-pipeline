#!/usr/bin/env python

import os
import sys
import fnmatch
import subprocess

## prepare to run PyTest as a command
from distutils.core import Command

from setuptools import setup, find_packages

from version import get_git_version
VERSION, SOURCE_LABEL = get_git_version()
PROJECT = 'streamcorpus_pipeline'
AUTHOR = 'Diffeo, Inc.'
AUTHOR_EMAIL = 'support@diffeo.com'
URL = 'http://github.com/trec-kba/streamcorpus-pipeline'
DESC = 'Tools for building streamcorpus objects, such as those used in TREC.'

def read_file(file_name):
    file_path = os.path.join(
        os.path.dirname(__file__),
        file_name
        )
    return open(file_path).read()

def recursive_glob(treeroot, pattern):
    results = []
    for base, dirs, files in os.walk(treeroot):
        goodfiles = fnmatch.filter(files, pattern)
        results.extend(os.path.join(base, f) for f in goodfiles)
    return results

def recursive_glob_with_tree(treeroot, pattern):
    results = []
    for base, dirs, files in os.walk(treeroot):
        goodfiles = fnmatch.filter(files, pattern)
        one_dir_results = []
        for f in goodfiles:
            one_dir_results.append(os.path.join(base, f))
        results.append((base, one_dir_results))
    return results

def _myinstall(pkgspec):
    setup(
        script_args = ['-q', 'easy_install', '-v', pkgspec],
        script_name = 'easy_install'
    )

class PyTest(Command):
    '''run py.test'''

    description = 'runs py.test to execute all tests'

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        if self.distribution.install_requires:
            for ir in self.distribution.install_requires:
                _myinstall(ir)
        if self.distribution.tests_require:
            for ir in self.distribution.tests_require:
                _myinstall(ir)

        # reload sys.path for any new libraries installed
        import site
        site.main()
        print sys.path
        # use pytest to run tests
        pytest = __import__('pytest')
        if pytest.main(['-n', '8', '-vvs', 'src']):
            sys.exit(1)

setup(
    name=PROJECT,
    version=VERSION,
    description=DESC,
    license=read_file('LICENSE.txt'),
    long_description=read_file('README.rst'),
    #source_label=SOURCE_LABEL,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    cmdclass={'test': PyTest,
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License',  ## MIT/X11 license http://opensource.org/licenses/MIT
    ],
    tests_require=[
        'pytest',
        'ipdb',
        'pytest-cov',
        'pytest-xdist',
        'pytest-timeout',
        'pytest-incremental',
        'pytest-capturelog',
        'epydoc',
    ],
    install_requires=[
        'yakonfig',
        'python-magic',
        'python-docx',
        'thrift',
        'gevent',
        'kvlayer >= 0.2.8',
        'rejester',
        'protobuf',
        'requests',
        'streamcorpus>=0.3.23',
        'pyyaml',
        'nltk',
        'lxml',
        'BeautifulSoup',
        'boto',
        'jellyfish',
        'nilsimsa>=0.2',
        'chromium_compact_language_detector',
        'sortedcollection',
        'python-docx',
        'pdfminer',
    ],
    entry_points={
        'console_scripts': [
            'streamcorpus_pipeline = streamcorpus_pipeline.run:main',
            'streamcorpus_pipeline_work_units = streamcorpus_pipeline._rejester:make_work_units',
        ]
    },
    data_files = [
        ## this does not appear to actually put anything into the egg...
        ('examples', recursive_glob('src/examples', '*.py')),
        ('configs', recursive_glob('configs', '*.yaml')),
        #('data/john-smith', recursive_glob('data/john-smith', '*.*')),
    ] + recursive_glob_with_tree('data/john-smith/original', '*')
)
