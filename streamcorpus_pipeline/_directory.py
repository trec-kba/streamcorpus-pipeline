'''Process entire directories using streamcorpus_pipeline.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

streamcorpus_directory
======================

:command:`streamcorpus_directory` reads an entire directory through
the streamcorpus pipeline.  This allows work to be distributed using
the :mod:`rejester` distributed work system, with an implied
assumption that all systems running a rejester worker for the given
namespace have access to the local file storage space, either because
the only worker is on the local system or because the named directory
is on a shared networked filesystem.

.. program:: streamcorpus_directory

.. option:: -c <yaml>, --config <yaml>

   Specify a YAML configuration file.

.. option:: --standalone

   Process all of the files in the directory, sequentially, then exit.
   This may be more convenient for debugging the pipeline configuration.
   Does not require rejester configuration.

.. option:: --rejester

   Create :mod:`rejester` work units, then exit.  The work spec will
   be named ``streamcorpus_directory``.  The configuration must contain
   complete :mod:`rejester` configuration.  This is the default mode.

Module Contents
===============

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
    default_config = { 'engine': 'rejester' }
    @staticmethod
    def add_arguments(parser):
        parser.add_argument(
            '--standalone',
            action='store_const', dest='engine', const='standalone',
            help='process directory locally in one batch')
        parser.add_argument(
            '--rejester',
            action='store_const', dest='engine', const='rejester',
            help='distribute processing using rejester')
    runtime_keys = { 'engine': 'engine' }
    @staticmethod
    def check_config(config, name):
        if ('engine' not in config or
            config['engine'] not in ['rejester', 'standalone']):
            raise yakonfig.ConfigurationError(
                'invalid {} engine type {!r}'.format(name, config['engine']))
        if config['engine'] == 'rejester':
            yakonfig.check_toplevel_config(rejester, name)
        yakonfig.check_toplevel_config(streamcorpus_pipeline, name)

def main():
    parser = argparse.ArgumentParser(conflict_handler='resolve',
        description='process entire directories using streamcorpus_pipeline')
    parser.add_argument('directories', nargs='+', metavar='directory',
                        help='directory name(s) to process')
    args = yakonfig.parse_args(parser, [yakonfig, rejester, kvlayer,
                                        streamcorpus_pipeline, DirectoryConfig])
    gconfig = yakonfig.get_global_config()
    dblogger.configure_logging(gconfig)
    
    work_spec = {
        'name': 'streamcorpus_directory',
        'desc': 'read files from a directory',
        'min_gb': 8,
        'config': gconfig,
        'module': 'streamcorpus_pipeline._rejester',
        'run_function': 'rejester_run_function',
        'terminate_function': 'rejester_terminate_function',
    }
    # There is surely a way to do this "forward" in one
    # iterator-only pass...but itertools is a little tricky
    work_units = {}
    for d in args.directories:
        for dirpath, dirnames, filenames in os.walk(d):
            work_units.update({
                os.path.abspath(os.path.join(dirpath, filename)): {
                    'start_count': 0,
                }
                for filename in filenames
            })

    if gconfig['streamcorpus_directory']['engine'] == 'rejester':
        tm = rejester.TaskMaster(gconfig['rejester'])
        tm.update_bundle(work_spec, work_units)
    elif gconfig['streamcorpus_directory']['engine'] == 'standalone':
        for k,v in work_units.iteritems():
            u = SimpleWorkUnit(k)
            u.spec = work_spec
            u.data = v
            rejester_run_function(u)
    
if __name__ == '__main__':
    main()
