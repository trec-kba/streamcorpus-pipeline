'''Stream item processing framework.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

The streamcorpus pipeline is a basic framework for processing
:class:`streamcorpus.StreamItem` objects.  Stream items are passed
through the pipeline, with different
:mod:`~streamcorpus_pipeline.stages` incrementally adding parts to it.
The general flow of the pipeline is as follows:

1. A *reader* stage produces an initial set of stream items, one per
   input document.  Many readers, such as
   :mod:`~streamcorpus_pipeline._local_storage.from_local_chunks` and
   :mod:`~streamcorpus_pipeline._s3_storage.from_s3_chunks`, expect to
   take in :mod:`streamcorpus.Chunk` files with fully populated stream
   items.
   :mod:`~streamcorpus_pipeline._yaml_files_list.yaml_files_list` takes
   a listing of plain external files from a YAML file.  Other
   processing may require writing a custom reader stage.  In the most
   basic case, this produces a stream item with only the ``body.raw``
   field set.

2. A set of *transforms* annotates and fills in parts of the stream
   item.  If the inbound documents contain only raw content, it is
   common to run :mod:`~streamcorpus_pipeline._language.language` to
   detect the language, and to run dedicated stages to produce
   :mod:`~streamcorpus_pipeline._clean_html.clean_html` and
   :mod:`~streamcorpus_pipeline._clean_visible.clean_visible` forms of
   the body.  Annotators such as
   :mod:`~streamcorpus_pipeline._hyperlink_labels.hyperlink_labels`
   will add labels at known offsets within the entire document.

3. A *tokenizer* reads the actual text and parses it into parts of
   speech, creating token objects in the stream item.  A tokenizer
   is not included in this package, but there are bundled stages
   to run :mod:`~streamcorpus_pipeline._tokenizer.nltk_tokenizer`,
   :mod:`~streamcorpus_pipeline._lingpipe.lingpipe`, and
   :mod:`~streamcorpus_pipeline._serif.serif`.

4. An *aligner* reads labels at known offsets in the document and
   the output of the tokenizer and attempts to move the labels to
   be attached to their corresponding tokens.  The aligner is generally
   run as part of the tokenizer rather than as a separate stage.

5. Additional transforms could be run at this point on the tokenized
   output, though this is rare.

6. A *writer* places the fully processed stream items somewhere.
   :mod:`~streamcorpus_pipeline._local_storage.to_local_chunks` is
   useful for local testing and development; networked storage via
   :mod:`~streamcorpus_pipeline._kvlayer.to_kvlayer` or
   :mod:`~streamcorpus_pipeline._s3_storage.to_s3_chunks` is helpful
   for publishing the chunk files to other developers and tools.

:command:`streamcorpus_pipeline`
================================

.. automodule:: streamcorpus_pipeline.run

:command:`streamcorpus_directory`
=================================

.. automodule:: streamcorpus_pipeline._directory

Pipeline configuration and execution
====================================

.. automodule:: streamcorpus_pipeline._pipeline

Pipeline stages
===============

.. automodule:: streamcorpus_pipeline.stages

:mod:`coordinate` integration
=============================

.. automodule:: streamcorpus_pipeline._coordinate

'''
from config import config_name, default_config, runtime_keys, sub_modules, \
    replace_config, check_config, normalize_config
from _pipeline import Pipeline, PipelineFactory
from _coordinate import coordinate_run_function, coordinate_terminate_function
from streamcorpus_pipeline._clean_visible import cleanse
