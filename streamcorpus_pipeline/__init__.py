'''Stream item processing framework.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

The streamcorpus pipeline is a basic framework for processing
:class:`streamcorpus.StreamItem` objects.

:command:`streamcorpus_pipeline`
================================

.. automodule:: streamcorpus_pipeline.run

Pipeline configuration and execution
====================================

.. automodule:: streamcorpus_pipeline._pipeline

Pipeline stages
===============

.. automodule:: streamcorpus_pipeline.stages

:mod:`rejester` integration
===========================

.. automodule:: streamcorpus_pipeline._rejester

'''
from config import config_name, default_config, runtime_keys, sub_modules, \
    replace_config, check_config, normalize_config
from _pipeline import Pipeline, PipelineFactory
from _rejester import rejester_run_function, rejester_terminate_function
