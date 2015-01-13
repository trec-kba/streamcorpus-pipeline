'''Process entire directories using streamcorpus_pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014-2015 Diffeo, Inc.

:command:`streamcorpus_directory` reads an entire directory through
the streamcorpus pipeline.  This allows work to be distributed using
the :mod:`rejester` distributed work system, with an implied
assumption that all systems running a rejester worker for the given
namespace have access to the local file storage space, either because
the only worker is on the local system or because the named directory
is on a shared networked filesystem.

The command is followed by a list of directory names, or other inputs.
By default each named directory is scanned, and each file in each
directory is submitted as an input to the pipeline.  A :mod:`rejester`
job is created for each file.

Note that :mod:`rejester` is typically used to distribute jobs acress
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
   Does not require rejester configuration.

.. option:: --rejester

   Create :mod:`rejester` work units, then exit.  The configuration
   must contain complete :mod:`rejester` configuration.  This is the
   default mode.

.. option:: --work-spec <name>

   Use `name` as the name of the :mod:`rejester` work spec.  Defaults
   to ``streamcorpus_directory``.

.. option:: --nice <n>

   Deprioritize the submitted jobs in :mod:`rejester`.  `n` can be
   a relative score between 0 and 20.

'''
from __future__ import absolute_import
import argparse
import os

import dblogger
import kvlayer
import rejester
import streamcorpus_pipeline
from streamcorpus_pipeline._rejester import rejester_run_function
from streamcorpus_pipeline.run import SimpleWorkUnit
import yakonfig


class DirectoryConfig(object):
    '''Configuration metadata for the directory scanner.'''
    config_name = 'streamcorpus_directory'
    default_config = {
        'engine': 'rejester',
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
            '--rejester',
            action='store_const', dest='engine', const='rejester',
            help='distribute processing using rejester (default)')
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
            '--work-spec', help='name of rejester work spec')

    runtime_keys = {
        'engine': 'engine',
        'mode': 'mode',
        'work_spec': 'name',
    }

    @staticmethod
    def check_config(config, name):
        if (('engine' not in config or
             config['engine'] not in ['rejester', 'standalone'])):
            raise yakonfig.ConfigurationError(
                'invalid {} engine type {!r}'
                .format(name, config.get('engine')))
        if (('mode' not in config or
             config['mode'] not in ['directories', 'files', 'file-lists'])):
            raise yakonfig.ConfigurationError(
                'invalid {} mode {!r}'.format(name, config.get('mode')))
        if config['engine'] == 'rejester':
            yakonfig.check_toplevel_config(rejester, name)
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
        parser, [yakonfig, rejester, kvlayer, dblogger,
                 streamcorpus_pipeline, DirectoryConfig])
    gconfig = yakonfig.get_global_config()
    scdconfig = gconfig['streamcorpus_directory']

    work_spec = {
        'name': scdconfig.get('name', 'streamcorpus_directory'),
        'desc': 'read files from a directory',
        'min_gb': 1,
        'config': gconfig,
        'module': 'streamcorpus_pipeline._rejester',
        'run_function': 'rejester_run_function',
        'terminate_function': 'rejester_terminate_function',
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

    work_units = {filename: {'start_count': 0}
                  for filename in get_filenames()}

    if scdconfig['engine'] == 'rejester':
        tm = rejester.build_task_master(gconfig['rejester'])
        tm.update_bundle(work_spec, work_units, nice=args.nice)
    elif scdconfig['engine'] == 'standalone':
        for k, v in work_units.iteritems():
            u = SimpleWorkUnit(k)
            u.spec = work_spec
            u.data = v
            rejester_run_function(u)

if __name__ == '__main__':
    main()
