'''Process entire directories using streamcorpus_pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014-2015 Diffeo, Inc.

:command:`streamcorpus_directory` reads an entire directory through
the streamcorpus pipeline.  This allows work to be distributed using
the :mod:`coordinate` distributed work system, with an implied
assumption that all systems running a coordinate worker for the given
namespace have access to the local file storage space, either because
the only worker is on the local system or because the named directory
is on a shared networked filesystem.

The command is followed by a list of directory names, or other inputs.
By default each named directory is scanned, and each file in each
directory is submitted as an input to the pipeline.  A :mod:`coordinate`
job is created for each file.

Note that :mod:`coordinate` is typically used to distribute jobs acress
a cluster of systems.  This program only submits the file names as
jobs; it does not submit actual file contents.  If the pipeline is
configured with a reader such as
:mod:`~streamcorpus_pipeline._s3_storage.from_s3_chunks`, this tool
can be used to submit a sequence of S3 source paths, either on the
command line directly using the :option:`--files
<streamcorpus_directory --files>` option, or by collecting the paths
into a text file and using :option:`--file-lists
<streamcorpus_directory --file-lists>`.  If the pipeline is configured
with a reader such as
:mod:`~streamcorpus_pipeline._local_storage.from_local_chunks`, the
caller must ensure that every system running a worker process has a
copy of the source files.

This supports the standard :option:`--config <yakonfig --config>`,
:option:`--dump-config <yakonfig --dump-config>`, :option:`--verbose
<dblogger --verbose>`, :option:`--quiet <dblogger --quiet>`, and
:option:`--debug <dblogger --debug>` options.  It supports the
following additional options:

.. program:: streamcorpus_directory

.. option:: --directories

   The positional arguments are interpreted as directory names.  Each
   named directory is scanned recursively, and every file in or under
   that directory is used as an input to the pipeline.  This is the
   default mode.

.. option:: --files

   The positional arguments are interpreted as file names.  Each named
   file is used as an input to the pipeline.

.. option:: --file-lists

   The positional arguments are file names.  Each named file is a text
   file containing a list of input filenames or other sources, one to
   a line.  These are used as inputs to the pipeline.

.. option:: --standalone

   Process all of the files in the directory, sequentially, then exit.
   This may be more convenient for debugging the pipeline configuration.
   Does not require coordinate configuration.

.. option:: --coordinate

   Create :mod:`coordinate` work units, then exit.  The configuration
   must contain complete :mod:`coordinate` configuration.  This is the
   default mode.

.. option:: --work-spec <name>

   Use `name` as the name of the :mod:`coordinate` work spec.  Defaults
   to ``streamcorpus_directory``.

.. option:: --nice <n>

   Deprioritize the submitted jobs in :mod:`coordinate`.  `n` can be
   a relative score between 0 and 20.

'''
from __future__ import absolute_import, print_function, division
import argparse
from itertools import islice
import json
import logging
import os

import dblogger
import kvlayer
import coordinate
import streamcorpus_pipeline
from streamcorpus_pipeline._coordinate import coordinate_run_function
from streamcorpus_pipeline.run import SimpleWorkUnit
import yakonfig

logging.basicConfig()
logger = logging.getLogger(__name__)


class DirectoryConfig(object):
    '''Configuration metadata for the directory scanner.'''
    config_name = 'streamcorpus_directory'
    default_config = {
        'engine': 'coordinate',
        'mode': 'directories',
        'name': 'streamcorpus_directory',
    }

    @staticmethod
    def add_arguments(parser):
        parser.add_argument(
            '--standalone',
            action='store_const', dest='engine', const='standalone',
            help='process directory locally in one batch')
        parser.add_argument(
            '--coordinate',
            action='store_const', dest='engine', const='coordinate',
            help='distribute processing using coordinate (default)')
        parser.add_argument(
            '--directories',
            action='store_const', dest='mode', const='directories',
            help='arguments are directories (default)')
        parser.add_argument(
            '--files',
            action='store_const', dest='mode', const='files',
            help='arguments are individual files')
        parser.add_argument(
            '--file-lists',
            action='store_const', dest='mode', const='file-lists',
            help='arguments are files containing lists of files')
        parser.add_argument(
            '--work-spec', help='name of coordinate work spec')

    runtime_keys = {
        'engine': 'engine',
        'mode': 'mode',
        'work_spec': 'name',
    }

    @staticmethod
    def check_config(config, name):
        if (('engine' not in config or
             config['engine'] not in ['coordinate', 'standalone'])):
            raise yakonfig.ConfigurationError(
                'invalid {0} engine type {1!r}'
                .format(name, config.get('engine')))
        if (('mode' not in config or
             config['mode'] not in ['directories', 'files', 'file-lists'])):
            raise yakonfig.ConfigurationError(
                'invalid {0} mode {1!r}'.format(name, config.get('mode')))
        if config['engine'] == 'coordinate':
            yakonfig.check_toplevel_config(coordinate, name)
        yakonfig.check_toplevel_config(streamcorpus_pipeline, name)


def main():
    parser = argparse.ArgumentParser(
        conflict_handler='resolve',
        description='process entire directories using streamcorpus_pipeline')
    parser.add_argument('directories', nargs='+', metavar='directory',
                        help='directory name(s) to process')
    parser.add_argument('-n', '--nice', default=0, type=int,
                        help='specify a nice level for these jobs')
    args = yakonfig.parse_args(
        parser, [yakonfig, coordinate, kvlayer, dblogger,
                 streamcorpus_pipeline, DirectoryConfig])
    gconfig = yakonfig.get_global_config()
    scdconfig = gconfig['streamcorpus_directory']

    work_spec = {
        'name': scdconfig.get('name', 'streamcorpus_directory'),
        'desc': 'read files from a directory',
        'min_gb': 1,
        'config': gconfig,
        'module': 'streamcorpus_pipeline._coordinate',
        'run_function': 'coordinate_run_function',
        'terminate_function': 'coordinate_terminate_function',
    }

    def get_filenames():
        for d in args.directories:
            if scdconfig['mode'] == 'files':
                yield d
            elif scdconfig['mode'] == 'file-lists':
                with open(d, 'r') as f:
                    for line in f:
                        yield line.strip()
            elif scdconfig['mode'] == 'directories':
                for dirpath, dirnames, filenames in os.walk(d):
                    for filename in filenames:
                        yield os.path.abspath(os.path.join(dirpath, filename))

    v = {'start_count': 0}

    if scdconfig['engine'] == 'coordinate':
        print('creating coordinate TaskMaster:\n%s' % json.dumps(gconfig['coordinate'], indent=4, sort_keys=True))
        tm = coordinate.TaskMaster(gconfig['coordinate'])
        fnames = list(get_filenames())
        it = iter(fnames)
        count = 0
        while 1:
            chunk = {filename: v for filename in islice(it, 5000)}
            if not chunk: break
            tm.update_bundle(work_spec, chunk, nice=args.nice)
            count += len(chunk)
            print('loaded %d of %d WorkUnits' % (count, len(fnames)))
    elif scdconfig['engine'] == 'standalone':
        for filename in get_filenames():
            u = SimpleWorkUnit(filename)
            u.spec = work_spec
            u.data = v
            coordinate_run_function(u)

if __name__ == '__main__':
    main()
