Overview of StreamCorpus software components
============================================

StreamCorpus provides a toolkit for processing massive streams of text
and running natural language extractors on the text.  StreamCorpus
does not provide any extractors itself; it operates third-party
extractors, such as Serif and Factorie, and unifies their output.
StreamCorpus pipline can store documents in S3, Accumulo, or flat
files.

StreamCorpus is used extensively in `TREC KBA <http://trec-kba.org/>`_, 
`TREC Temporal Summarization <http://trec-ts.org/>`_, 
`TREC Dynamic Domain <http://trec-dd.org/>`_, 
and at `Diffeo <http://diffeo.com/>`_.

Ask more at `StreamCorpus Google Group <http://groups.google.com/group/streamcorpus>`_

All of these python package are hosted in `github.com/trec-kba <http://github.com/trec-kba/>`_ 
and `github.com/diffeo <http://github.com/diffeo/>`_

Invocation
----------

The general data flow is as follows:

1. Convert your input format into :class:`streamcorpus.StreamItem`
   format.  This generally needs to be done by custom code
   implementing the :mod:`streamcorpus_pipeline` reader stage
   interface.

2. Run :program:`streamcorpus_pipeline` over the original inputs to
   produce :class:`streamcorpus.Chunk` files, either stored locally or
   in :mod:`kvlayer` backed storage.

All of the programs share a common configuration interface.  You can
pass :option:`--dump-config <yakonfig --dump-config>` to any of the
programs to see the default configuration, and :option:`--config
<yakonfig --config>` to any of them to provide your own configuration
file.

Example
-------

One convenient path to load data is to use the
:class:`~streamcorpus_pipeline._yaml_files_list.yaml_files_list`
reader to load in plain-text data files matching known entities.  We
will load the data into Apache Accumulo as a backing database.  Create
a shared configuration file, :file:`common.yaml`, that includes the
basic shared setup, as well as some basic logging configuration and
support for the :mod:`rejester` distributed computing environment:

.. code-block:: yaml

    logging:
      root:
        level: INFO
    kvlayer:
      app_name: datasets
      namespace: mydataset
      storage_type: accumulo
      storage_addresses: [ "accumulo-proxy.example.com:50096" ]
      username: root
      password: secret
   rejester: # necessary but unused in this example
     app_name: datasets
     namespace: mydataset
     registry_addresses: [ "redis.example.com:6379" ]

The reader needs a specific YAML file to tell it where to find input
documents and how to label them.  This file, :file:`labels.yaml`,
looks like:

.. code-block:: yaml

    root_path:                # "empty" means working directory
    source: source            # embedded in StreamItem.source
    annotator_id: annotator   # embedded in labels
    entities:
      - target_id: https://kb.diffeo.com/entity
        doc_path: data
        slots:
          - canonical_name: Entity
          - entity

This will cause the reader to read the documents under the
:file:`data` path, create a stream item for each marked as coming from
``source``, and search each for appearances of the term "entity".
Mentions of that term will be labelled as corresponding to the
``https://kb.diffeo.com/entity`` entity, according to the annotator
"annotator".

A :program:`streamcorpus_pipeline` configuration that reads this using
the Serif NLP tagger can be stored in
:file:`streamcorpus_pipeline.yaml`:

.. code-block:: yaml

    # ... paste common.yaml here ...

    streamcorpus_pipeline:
      third_dir_path: /third
      tmp_dir_path: tmp
      output_chunk_max_count: 500
      reader: yaml_files_list
      incremental_transforms:
        - language
        - guess_media_type
        - clean_html
        - hyperlink_labels
        - clean_visible
      batch_transforms: [ serif ]
      writers: [ to_kvlayer ]
      hyperlink_labels:
        require_abs_url: true
        all_domains: true
        offset_types: [ BYTES, CHARS ]
      serif:
        path_in_third: serif/serif-latest
        cleanup_tmp_files: true
        par: streamcorpus_one_step
        align_labels_by: names_in_chains
        aligner_data:
          chain_selector: ANY_MULTI_TOKEN
          annotator_id: annotator

Then you can run

.. code-block:: bash

    streamcorpus_pipeline \
      --config streamcorpus_pipeline.yaml --input labels.yaml

Module dependencies
-------------------

.. digraph:: modules

   streamcorpus_pipeline -> streamcorpus
   streamcorpus_pipeline -> yakonfig
   streamcorpus_pipeline -> kvlayer [style=dotted]
   streamcorpus_pipeline -> dblogger
   streamcorpus_pipeline -> rejester [style=dotted]
   rejester -> yakonfig
   kvlayer -> yakonfig
