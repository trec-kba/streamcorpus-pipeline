#!/usr/bin/env python
'''streamcorpus_pipeline.BatchTransform for Serif

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import
import gc
import logging
import os
import shutil
import subprocess
import time
import traceback
import uuid

from yakonfig import ConfigurationError

from streamcorpus_pipeline.stages import BatchTransform
from streamcorpus_pipeline._taggers import make_memory_info_msg, align_labels
from streamcorpus_pipeline._exceptions import PipelineOutOfMemory, \
    PipelineBaseException

logger = logging.getLogger(__name__)


class serif(BatchTransform):
    '''Batch transform to parse and tag documents with Serif.

    Serif must be installed externally, somewhere underneath the
    directory configured as `third_dir_path` in the top-level
    pipeline configuration.

    A typical Serif pipeline configuration looks like:

    .. code-block:: yaml

        streamcorpus_pipeline:
          tmp_dir_path: tmp
          third_dir_path: /opt/diffeo/third
          reader: ...
          incremental_transforms:
            - language
            - guess_media_type
            - clean_html
            - clean_visible
          batch_transforms: [serif]
          writers: [...]

          serif:
            path_in_third: serif/serif-latest
            par: streamcorpus_one_step
            align_labels_by: names_in_chains
            aligner_data:
              chain_selector: ALL
              annotator_id: annotator

    Configuration options include:

    `path_in_third` (required)
      Relative path in `third_dir_path` to directory containing serif
      data directories

    `serif_exe` (default: bin/x86_64/Serif)
      Relative path within `path_in_third` to the Serif executable file

    `par` (default: streamcorpus_one_step)
      Serif policy configuration; this is typically ``streamcorpus_one_step``,
      but may also be ``streamcorpus_read_serifxml`` or
      ``streamcorpus_generate_serifxml``

    `par_additions`
      Map from poicy configuration name to list of strings to append
      as additional lines to customize the policy.

    `cleanup_tmp_files` (default: true)
      Delete the intermediate files used by Serif

    The two "align" options control how ratings on the document
    are associated with tokens generated by Serif.

    '''
    config_name = 'serif'
    tagger_id = 'serif'

    default_config = {
        'path_in_third': 'serif/serif-latest',
        'par': 'streamcorpus_one_step',
        'par_additions': {},
        'cleanup_tmp_files': True,
        'serif_exe': 'bin/x86_64/Serif',
    }

    @staticmethod
    def check_config(config, name):
        # We expect the pipeline to force in tmp_dir_path and
        # third_dir_path; but these are required
        for k in ['path_in_third', 'serif_exe', 'par', 'cleanup_tmp_files']:
            if k not in config:
                raise ConfigurationError('{0} requires {1} setting'
                                         .format(name, k))

    # These two serif parameter files came from BBN via email,
    # the full docs are in ../../../docs/serif*txt

    serif_global_config = '''
#######################################################################
## Serif Global Configuration
#######################################################################
##
## This file tells Serif where it can find trained model files and the
## scripts that are used for scoring.  It is imported by each of the
## other parameter files (such as all.best-english.par).  It is not
## intended to be used directly.
##
## This file needs to be updated if Serif is moved or copied to a new
## location.  No other files should need to be changed.
##
#######################################################################

serif_home: ..

serif_data: %serif_home%/data
serif_score: %serif_home%/scoring
server_docs_root: %serif_home%/doc/http_server
use_feature_module_BasicCipherStream: true
cipher_stream_always_decrypt: true
use_feature_module_KbaStreamCorpus: true
use_feature_module_HTMLDocumentReader: true
html_character_entities: %serif_data%/unspec/misc/html-character-entities.listmap
log_threshold: ERROR

#document_split_strategy: region
#document_splitter_max_size:  20000
'''

    streamcorpus_one_step = '''

### WARNING: this was written by the streamcorpus_pipeline, and will be
### overwritten the next time the streamcorpus_pipeline runs.

# Usage: Serif streamcorpus_one_step.par CHUNK_FILES -o OUT_DIR
#
# This parameter file is used for running SERIF from start-to-end on
# streamcorpus chunk files.
#
# Using this parameter file, SERIF will read each of the specified
# chunk files, and will generate corresponding chunk files in the
# directory OUT_DIR/output.  Stream items whose language is English
# (or unspecified) will be augmented with SERIF annotations.  The
# input text for SERIF will be read from clean_html (when non-empty)
# or from clean_visible (otherwise).

INCLUDE {serif_home_par}/config.par
INCLUDE {serif_home_par}/master.english.par
INCLUDE {serif_home_par}/master.english-speed.par
OVERRIDE use_feature_module_KbaStreamCorpus:  true
OVERRIDE use_stream_corpus_driver:            true
OVERRIDE start_stage:                         START
OVERRIDE end_stage:                           output
OVERRIDE kba_write_results_to_chunk:          true
OVERRIDE parser_skip_unimportant_sentences:   false

OVERRIDE input_type:                          rawtext
OVERRIDE source_format:            sgm

OVERRIDE document_split_strategy: region
OVERRIDE document_splitter_max_size: 20000
#OVERRIDE log_force: kba-stream-corpus,profiling
#OVERRIDE kba_skip_docs_with_empty_language_code: false
'''

    streamcorpus_read_serifxml = '''

### WARNING: this was written by the streamcorpus_pipeline, and will be
### overwritten the next time the streamcorpus_pipeline runs.

# Usage: Serif streamcorpus_read_serifxml.par CHUNK_FILES -o OUT_DIR
#
# This parameter file is used in cases where SERIF has already been
# run on each stream item, and the SerifXML is stored in the tagging.
# This version does not need to load any SERIF models, since it
# expects that all serif processing has already been done.
#
# Using this parameter file, SERIF will read each of the specified
# chunk files, and will generate corresponding chunk files in the
# directory OUT_DIR/output.  Stream items whose language is English
# (or unspecified) are expected to contain a value for the field
# StreamItem.body.taggings['serif'].raw_tagging, containing fully
# processed SerifXML output for that item.  This SerifXML output will
# be read, and then corresponding sentence and relation annotations
# will be added to the output files.

INCLUDE {serif_home_par}/config.par
INCLUDE {serif_home_par}/master.english.par
INCLUDE {serif_home_par}/master.english-speed.par
OVERRIDE use_feature_module_KbaStreamCorpus:  true
OVERRIDE use_stream_corpus_driver:            true
OVERRIDE start_stage:                         output
OVERRIDE end_stage:                           output
OVERRIDE kba_read_serifxml_from_chunk:        true
OVERRIDE kba_write_results_to_chunk:          true
OVERRIDE parser_skip_unimportant_sentences:   false
OVERRIDE source_format:                       serifxml
'''

    streamcorpus_generate_serifxml = '''

### WARNING: this was written by the streamcorpus_pipeline, and will be
### overwritten the next time the streamcorpus_pipeline runs.

# Usage: Serif streamcorpus_generate_serifxml.par CHUNK_FILES -o OUT_DIR
#
# This parameter file is identical to streamcorpus_one_step.par, except
# that: (1) it saves the serifxml output to the stream items; and (2)
# it does *not* save the sentence & relation annotations.  I used this
# parameter file to generate test inputs for the
# streamcorpus_read_serifxml.par parameter file.

# (streamcorpus_one_step.par is defined above)
INCLUDE ./streamcorpus_one_step.par
OVERRIDE kba_write_results_to_chunk:          false
OVERRIDE kba_write_serifxml_to_chunk:         true
'''

    def __init__(self, *args, **kwargs):
        super(serif, self).__init__(*args, **kwargs)
        self.tagger_root_path = os.path.join(self.config['third_dir_path'],
                                             self.config['path_in_third'])
        self._child = None

    def _write_config_par(self, tmp_dir, par_file):
        if par_file == 'streamcorpus_generate_serifxml':
            # streamcorpus_generate_serifxml INCLUDEs streamcorpus_one_step
            self._write_config_par(tmp_dir, 'streamcorpus_one_step',
                                   self.tagger_root_path)
        par_data = getattr(self, par_file)
        for line in self.config.get('par_additions', {}).get(par_file, []):
            par_data += '\n'
            par_data += line + '\n'
        fpath = os.path.join(tmp_dir, par_file + '.par')
        fout = open(fpath, 'wb')
        fout.write(
            par_data.format(
                serif_home_par=os.path.join(self.tagger_root_path, 'par')
            )
        )
        fout.close()
        logger.debug('wrote %s (%s bytes) to %r', par_file, len(par_data),
                     fpath)
        return fpath

    def process_path(self, chunk_path):
        tmp_dir = os.path.join(self.config['tmp_dir_path'], str(uuid.uuid4()))
        os.mkdir(tmp_dir)
        par_file = self.config['par']
        par_path = self._write_config_par(tmp_dir, par_file)

        tmp_chunk_path = os.path.join(tmp_dir, 'output',
                                      os.path.basename(chunk_path))

        cmd = [
            os.path.join(self.tagger_root_path, self.config['serif_exe']),
            par_path,
            '-o', tmp_dir,
            chunk_path,
        ]

        logger.info('serif cmd: %r', cmd)

        start_time = time.time()
        # make sure we are using as little memory as possible
        gc.collect()
        try:
            self._child = subprocess.Popen(cmd, stderr=subprocess.PIPE,
                                           shell=False)
        except OSError, exc:
            logger.error('error running serif cmd %r', cmd, exc_info=True)
            msg = traceback.format_exc(exc)
            msg += make_memory_info_msg()
            logger.critical(msg)
            raise

        s_out, errors = self._child.communicate()

        if not self._child.returncode == 0:
            if self._child.returncode == 137:
                msg = 'tagger returncode = 137\n' + errors
                msg += make_memory_info_msg()
                # maybe get a tail of /var/log/messages
                raise PipelineOutOfMemory(msg)
            elif 'Exception' in errors:
                logger.error('child code %s errorlen=%s',
                             self._child.returncode, len(errors))
                raise PipelineBaseException(errors)
            else:
                raise PipelineBaseException('tagger exited with %r' %
                                            self._child.returncode)

        # generated new tokens, so align labels with them
        align_labels(tmp_chunk_path, self.config)

        if self.config.get('cleanup_tmp_files', True):
            # default: cleanup tmp directory
            os.rename(tmp_chunk_path, chunk_path)
            shutil.rmtree(tmp_dir)
        else:
            # for development, no cleanup, leave tmp_file
            chunk_path_save = ('{0}_pre_serif_{1}'
                               .format(chunk_path,
                                       os.path.getmtime(chunk_path)))
            os.rename(chunk_path, chunk_path_save)
            shutil.copy(tmp_chunk_path, chunk_path)

        elapsed = time.time() - start_time
        logger.info('finished tagging in %.1f seconds' % elapsed)
        return elapsed

    def shutdown(self):
        '''
        send SIGTERM to the tagger child process
        '''
        if self._child:
            try:
                self._child.terminate()
            except OSError, exc:
                if exc.errno == 3:
                    # child is already gone, possibly because it ran
                    # out of memory and caused us to shutdown
                    pass
                else:
                    raise
