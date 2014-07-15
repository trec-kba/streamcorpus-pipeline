'''Support for running pipeline  tasks under :mod:`rejester`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

Since running pipeline stages can be expensive, particularly if busy
third-party stages are run, it can be desirable to run the pipeline
under the :mod:`rejester` distributed-computing framework.  This
module provides rejester "run" and "terminate" functions that run
the pipeline.  A typical rejester work spec will look like::

    work_spec = {
        'name': 'pipeline',
        'desc': 'run streamcorpus_pipeline',
        'min_gb': 8,
        'config': yakonfig.get_global_config(),
        'module': 'streamcorpus_pipeline._rejester',
        'run_function': 'rejester_run_function',
        'terminate_function': 'rejester_terminate_function',
    }

Work units have an input filename or other description as a key, and
any non-empty dictionary as their data.  They are processed using
:meth:`streamcorpus_pipeline.Pipeline._process_task`.  Pass the work
spec and work unit dictionaries to
:meth:`rejester.TaskMaster.update_bundle` to inject the work
definitions.

Note that this framework makes no effort to actually distribute the
data around.  Either only run rejester jobs on one system, or ensure
that all systems running rejester workers have a shared filesystem
such as NFS, or use network-based reader and writer stages such as
:class:`~streamcorpus_pipeline._s3_storage.from_s3_chunks` and
:class:`~streamcorpus_pipeline._kvlayer.to_kvlayer`.

.. autofunction:: rejester_run_function
.. autofunction:: rejester_terminate_function

'''
from __future__ import absolute_import
import argparse
import sys
import time

import json

import dblogger
import kvlayer
import streamcorpus_pipeline
from streamcorpus_pipeline.run import instantiate_config
from streamcorpus_pipeline._pipeline import PipelineFactory
from streamcorpus_pipeline._exceptions import ConfigurationError
from streamcorpus_pipeline.stages import PipelineStages
import yakonfig

def rejester_run_function(work_unit):
    with yakonfig.defaulted_config([dblogger, kvlayer, streamcorpus_pipeline],
                                   config=work_unit.spec.get('config', {})):
        scp_config = yakonfig.get_global_config('streamcorpus_pipeline')
        stages = PipelineStages()
        if 'external_stages_path' in scp_config:
            stages.load_external_stages(scp_config['external_stages_path'])
        if 'external_stages_modules' in scp_config:
            for mod in scp_config['external_stages_modules']:
                stages.load_module_stages(mod)
        factory = PipelineFactory(stages)
        pipeline = factory(scp_config)

        work_unit.data['start_chunk_time'] = time.time()
        work_unit.data['start_count'] = 0
        pipeline._process_task(work_unit)

def rejester_terminate_function(work_unit):
    pass
