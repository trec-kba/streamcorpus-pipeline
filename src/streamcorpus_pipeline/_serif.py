#!/usr/bin/env python
'''
streamcorpus_pipeline.BatchTransform for Serif

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import gc
import sys
import uuid
import time
import shutil
import logging
import traceback
import subprocess

from stages import BatchTransform
from _taggers import make_memory_info_msg, align_labels
from _exceptions import PipelineOutOfMemory, PipelineBaseException

logger = logging.getLogger(__name__)

class serif(BatchTransform):
    '''
    a kba.pipeline batch transform that converts a chunk into a new
    chunk with data generated by Serif
    '''
    tagger_id = 'serif'

    ## These two serif parameter files came from BBN via email,
    ## the full docs are in ../../../docs/serif*txt

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

    def __init__(self, config):
        self.config = config
        self._child = None

    def _write_config_par(self, tmp_dir, par_file, pipeline_root_path):
        if par_file == 'streamcorpus_generate_serifxml':
            # streamcorpus_generate_serifxml INCLUDEs streamcorpus_one_step
            self._write_config_par(tmp_dir, 'streamcorpus_one_step', pipeline_root_path)
        par_data = getattr(self, par_file)
        fpath = os.path.join(tmp_dir, par_file + '.par')
        fout = open(fpath, 'wb')
        fout.write(
            par_data.format(
                serif_home_par=os.path.join(pipeline_root_path, 'par')
            )
        )
        fout.close()
        logger.debug('wrote %s (%s bytes) to %r', par_file, len(par_data), fpath)
        return fpath

    def process_path(self, chunk_path):

        tmp_dir = os.path.join(self.config['tmp_dir_path'], str(uuid.uuid4()))
        os.mkdir(tmp_dir)
        par_file = self.config['par']
        pipeline_root_path = self.config['pipeline_root_path']

        par_path = self._write_config_par(tmp_dir, par_file, pipeline_root_path)

        tagger_config = dict(
            pipeline_root_path=self.config['pipeline_root_path'],
            par_file=self.config['par'],
            tmp_dir=tmp_dir,
            chunk_path=chunk_path,
            )

        tmp_chunk_path = os.path.join(tmp_dir, 'output', os.path.basename(chunk_path))

        cmd = [
            os.path.join(pipeline_root_path, 'bin/x86_64/Serif'),
            par_path,
            '-o', tmp_dir,
            chunk_path,
        ]

        logger.info('serif cmd: %r', cmd)

        start_time = time.time()
        ## make sure we are using as little memory as possible
        gc.collect()
        try:
            self._child = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=False)
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
                logger.error('child code %s errorlen=%s', self._child.returncode, len(errors))
                raise PipelineBaseException(errors)
            else:
                raise PipelineBaseException('tagger exited with %r' % self._child.returncode)

        ## generated new tokens, so align labels with them
        align_labels(tmp_chunk_path, self.config)

        if self.config.get('cleanup_tmp_files', True):
            ## default: cleanup tmp directory
            logger.info('atomic rename: %r --> %r', tmp_chunk_path, chunk_path)
            os.rename(tmp_chunk_path, chunk_path)
            logger.debug('done renaming, rm tmp_dir %r', tmp_dir)
            shutil.rmtree(tmp_dir)
        else:
            ## for development, no cleanup, leave tmp_file
            chunk_path_save = '{0}_pre_serif_{1}'.format(chunk_path, os.path.getmtime(chunk_path))
            logger.debug('save tmp %r -> %r', chunk_path, chunk_path_save)
            os.rename(chunk_path, chunk_path_save)
            logger.info('copy %r -> %r', tmp_chunk_path, chunk_path)
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
                    ## child is already gone, possibly because it ran
                    ## out of memory and caused us to shutdown
                    pass
