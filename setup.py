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

def recursive_glob_with_tree(new_base, old_base, treeroot, pattern):
    '''generate a list of tuples(new_base, list(paths to put there)
    where the files are found inside of old_base/treeroot.
    '''
    results = []
    old_cwd = os.getcwd()
    os.chdir(old_base)
    for rel_base, dirs, files in os.walk(treeroot):
        goodfiles = fnmatch.filter(files, pattern)
        one_dir_results = []
        for f in goodfiles:
            one_dir_results.append(os.path.join(old_base, rel_base, f))
        results.append((os.path.join(new_base, rel_base), one_dir_results))
    os.chdir(old_cwd)
    return results

def _myinstall(pkgspec):
    subprocess.check_call(['pip', 'install', pkgspec])

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
        if pytest.main(['-n', '8', '-vvs', 'streamcorpus_pipeline']):
            sys.exit(1)


setup(
    name=PROJECT,
    version=VERSION,
    description=DESC,
    license='MIT/X11 license http://opensource.org/licenses/MIT',
    long_description=read_file('README.md'),
    #source_label=SOURCE_LABEL,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages = find_packages(),
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
        'pytest-capturelog',

        ## this does not work in python2.6
        'pytest-incremental',

        'epydoc',
    ],
    install_requires=[
        ## these are used by streamcorpus_pipeline_test
        'pytest-diffeo >= 0.1.7',

        'dblogger >= 0.4.0',
        'yakonfig >= 0.4.2',
        'importlib',
        'python-magic',
        'python-docx',
        'thrift',
        'ftfy',
        'kvlayer >= 0.4.10',
        'coordinate',
        'protobuf < 3',
        'requests',
        'streamcorpus>=0.3.48',
        'pyyaml',
        'nltk',
        'lxml',
        'BeautifulSoup',
        'html5lib',
        'boto',
        'jellyfish',
        'nilsimsa >= 0.3',
        'regex != 2014.08.28',
        'chromium_compact_language_detector',
        'sortedcollection',
        'python-docx',
        'pdfminer',
        'backports.lzma!=0.0.4',
        'backport_collections', # for python2.6
    ],
    extras_require = {
        'keyword_indexing': [
            'many_stop_words >= 0.2.0',
            'mmh3',
        ],
    },
    entry_points={
        'console_scripts': [
            'streamcorpus_pipeline = streamcorpus_pipeline.run:main',
            'streamcorpus_directory = streamcorpus_pipeline._directory:main',
            'streamcorpus_pipeline_test = streamcorpus_pipeline.tests.run:main',
            'streamcorpus_pipeline_keywords = streamcorpus_pipeline.keywords:main',
            'scp-xpaths = streamcorpus_pipeline.run_xpaths:main',
        ]
    },
    data_files = [
        ('docs/examples/streamcorpus-pipeline/', recursive_glob('examples', '*.py')),
        ('docs/examples/streamcorpus-pipeline/', recursive_glob('examples', '*.yaml')),
        ('data/streamcorpus-pipeline/john-smith/', [
                'data/john-smith/john-smith-tagged-by-lingpipe-serif-0-197.sc.xz',
                'data/john-smith/ground-truth.yaml',
                ]),
    ] + recursive_glob_with_tree('data/streamcorpus-pipeline', 'data', 'john-smith/original', '*') \
      + recursive_glob_with_tree('data/streamcorpus-pipeline', 'data', 'test', '*'),
    include_package_data = True,
    zip_safe = False
)
