:mod:`rejester` --- Redis-based distributed work manager
========================================================

.. automodule:: rejester
    :members:
    :undoc-members:
    :show-inheritance:


Example: StreamCorpus Simple Filter Stage
-----------------------------------------

Rejester makes it easy to run large batches of small jobs, such as a
couple million runs of tagging batches of 500 documents with an NER
tagger or filtering to smaller sets of documents.

As a simple and highly relevant example, we illustrate how to write a
filter function as an external stage and run it in AWS EC2.

:mod:`streamcorpus_pipeline` has several built-in filters `source in github
<https://github.com/trec-kba/streamcorpus-pipeline/blob/master/streamcorpus_pipeline/_filters.py>`_.
You can create your own as external stages.  For example, see this `exact name match filter
<https://github.com/trec-kba/streamcorpus-pipeline/blob/master/examples/filters_exact_match.py>`_.

.. code-block:: py

    ## use the newer regex engine
    import regex as re
    import string

    ## make a unicode translation table to converts all punctuation to white space
    strip_punctuation = dict((ord(char), u" ") for char in string.punctuation)

    white_space_re = re.compile("\s+")

    def strip_string(s):
        'strips punctuation and repeated whitespace from unicode strings'
        return white_space_re.sub(" ", s.translate(strip_punctuation).lower())


    class filter_exact_match(object):
        'trivial string matcher using simple normalization and regex'

        config_name = 'filter_exact_match'

        def __init__(self, config):
            path = config.get('match_strings_path')
            match_strings = open(path).read().splitlines()
            match_strings = map(strip_string, map(unicode, match_strings))
            self.matcher = re.compile('(.|\n)*?(%s)' % '|'.join(match_strings), re.I)

        def __call__(self, si, context):
            'only pass StreamItems that match'
            if self.matcher.match(strip_string(si.body.clean_visible.decode('utf-8'))):
                return si


    ## this is how streamcorpus_pipeline finds the stage
    Stages = {'filter_exact_match': filter_exact_match}


To run this in rejester, you need to setup a `redis server
<http://redis.io/>`_ (use version 2.8 or newer), and put the hostname
in your yaml configuration file:

.. code-block:: yaml

    logging:
      root:
        level: INFO

    rejester:
      namespace: my_kba_filtering
      app_name: rejester
      registry_addresses: ["redis.example.com:6379"]

    streamcorpus_pipeline:
      ## "." means current working directory
      root_path: .

      tmp_dir_path: tmp
      cleanup_tmp_files: true

      external_stages_path: examples/filter_exact_match.py

      reader: from_s3_chunks

      incremental_transforms:
        ## remove all StreamItems that do not exactly match
        - filter_exact_match

      batch_transforms: []

      filter_exact_match:
        ## names ending in "_path" will be made absolute relative to the root_path
        match_strings_path: my_match_strings.txt

      writers: [to_s3_chunks]

      from_s3_chunks:
        ## put paths to your own keys here; these files must be just the
        ## access_key_id and secret_access_key strings without newlines
        aws_access_key_id_path:     /data/trec-kba/keys/trec-aws-s3.aws_access_key_id
        aws_secret_access_key_path: /data/trec-kba/keys/trec-aws-s3.aws_secret_access_key

        ## this is the location of the NIST's StreamCorpus
        bucket: aws-publicdatasets
        s3_path_prefix: trec/kba/kba-streamcorpus-2014-v0_3_0

        tries: 10
        input_format: streamitem
        streamcorpus_version: v0_3_0

        ## you need this key if you are processing NIST's StreamCorpus,
        ## which is encrypted
        gpg_decryption_key_path: /data/trec-kba/keys/trec-kba-rsa.gpg-key.private

      to_s3_chunks:

        ## put your own bucket and paths to your own keys here; these
        ## files must be just the access_key_id and secret_access_key
        ## strings without newlines
        aws_access_key_id_path:     /data/trec-kba/keys/trec-aws-s3.aws_access_key_id
        aws_secret_access_key_path: /data/trec-kba/keys/trec-aws-s3.aws_secret_access_key

        bucket: aws-publicdatasets
        s3_path_prefix: trec/kba/kba-streamcorpus-2014-v0_3_0-to-delete

        output_name: "%(date_hour)s/%(source)s-%(num)d-%(input_md5)s-%(md5)s"
        tries: 10
        cleanup_tmp_files: true

        ## you only need these keys if you are encrypting the data that
        ## you are putting into your bucket; you need both if you require
        ## verify_via_http, which fetches and decrypts to validate what
        ## was saved.
        gpg_decryption_key_path: /data/trec-kba/keys/trec-kba-rsa.gpg-key.private
        gpg_encryption_key_path: /data/trec-kba/keys/trec-kba-rsa.gpg-key.pub
        gpg_recipient: trec-kba

        verify_via_http: true


Then, you can run the following commands to put tasks into the rejester queue:

.. code-block: bash

    ## populate the task queue with jobs
    streamcorpus_directory -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml --file-lists list_of_s3_paths.txt

    ## try running one, to make sure it works locally
    rejester -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml run_one

    ## launch a MultiWorker to use all the CPUs on this machine:
    rejester -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml run_worker

    ## check on the status of your jobs
    rejester -c examples/streamcorpus-2014-v0_3_0-exact-match-example.yaml summary


If you are interested in SaltStack states for spinning up machines to
do this, reach out on streamcorpus@googlegroups.com
