'''Support for running pipeline  tasks under :mod:`coordinate`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

Since running pipeline stages can be expensive, particularly if busy
third-party stages are run, it can be desirable to run the pipeline
under the :mod:`coordinate` distributed-computing framework.  This
module provides coordinate "run" and "terminate" functions that run
the pipeline.  A typical coordinate work spec will look like::

    work_spec = {
        'name': 'pipeline',
        'desc': 'run streamcorpus_pipeline',
        'min_gb': 8,
        'config': yakonfig.get_global_config(),
        'module': 'streamcorpus_pipeline._coordinate',
        'run_function': 'coordinate_run_function',
        'terminate_function': 'coordinate_terminate_function',
    }

Work units have an input filename or other description as a key, and
any non-empty dictionary as their data.  They are processed using
:meth:`streamcorpus_pipeline.Pipeline._process_task`.  Pass the work
spec and work unit dictionaries to
:meth:`coordinate.TaskMaster.update_bundle` to inject the work
definitions.

Note that this framework makes no effort to actually distribute the
data around.  Either only run coordinate jobs on one system, or ensure
that all systems running coordinate workers have a shared filesystem
such as NFS, or use network-based reader and writer stages such as
:class:`~streamcorpus_pipeline._s3_storage.from_s3_chunks` and
:class:`~streamcorpus_pipeline._kvlayer.to_kvlayer`.

.. autofunction:: coordinate_run_function
.. autofunction:: coordinate_terminate_function

'''
from __future__ import absolute_import
import time

import dblogger
import kvlayer
import streamcorpus_pipeline
from streamcorpus_pipeline._pipeline import PipelineFactory
from streamcorpus_pipeline.stages import PipelineStages, Configured
import yakonfig


static_stages = None


def coordinate_run_function(work_unit):
    global static_stages
    # we can get away with one 'global' stages object. We don't
    # actually have substantially different config from work_unit to
    # work_unit, especially not within streamcoprus_pipeline wokr.
    if static_stages is None:
        static_stages = PipelineStages()
    stages = static_stages

    with yakonfig.defaulted_config([dblogger, kvlayer, streamcorpus_pipeline],
                                   config=work_unit.spec.get('config', {})):
        scp_config = yakonfig.get_global_config('streamcorpus_pipeline')
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


def coordinate_terminate_function(work_unit):
    pass


class to_work_units(Configured):
    '''Writer that puts references into coordinate'''
    config_name = 'to_work_units'
    default_config = {}

    def __init__(self, *args, **kwargs):
        super(to_kvlayer, self).__init__(*args, **kwargs)

    def __call__(self, t_path, name_info, i_str):
        work_unit_specs = []

        for si in streamcorpus.Chunk(t_path):
            si_key = key_for_stream_item(si)
            work_unit_specs.append( (si_key, {}, {}) )

        return work_unit_specs
