'''
Logging utilities for the streamcorpus_pipeline

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import os
import sys
import logging
import traceback
import streamcorpus

def log_full_file(stream_item, extension, log_path=None):
    '''
    Saves a full stream_item to a one-file-long chunk with a name
    'extension' added to help explain why it was saved.
    '''
    if log_path and stream_item and extension:
        fname = '%s-%s.sc' % (stream_item.stream_id, extension)
        fpath = os.path.join(log_path, fname)
        if os.path.exists(fpath):
            os.remove(fpath)
        l_chunk = streamcorpus.Chunk(path=fpath, mode='wb')
        l_chunk.add(stream_item)
        l_chunk.close()

        print 'created %s' % fpath

class FixedWidthFormatter(logging.Formatter):
    '''
    Provides fixed-width logging display, see:
    http://stackoverflow.com/questions/6692248/python-logging-string-formatting
    '''
    filename_width = 17
    levelname_width = 8

    def format(self, record):
        max_filename_width = self.filename_width - 3 - len(str(record.lineno))
        filename = record.filename
        if len(record.filename) > max_filename_width:
            filename = record.filename[:max_filename_width]
        a = "%s:%s" % (filename, record.lineno)
        record.fixed_width_filename_lineno = a.ljust(self.filename_width)
        levelname = record.levelname
        levelname_padding = ' ' * (self.levelname_width - len(levelname))
        record.fixed_width_levelname = levelname + levelname_padding
        return super(FixedWidthFormatter, self).format(record)


_log_level = logging.DEBUG


ch = logging.StreamHandler()
formatter = FixedWidthFormatter('%(asctime)s pid=%(process)d %(fixed_width_filename_lineno)s %(fixed_width_levelname)s %(message)s')
ch.setLevel(_log_level)
ch.setFormatter(formatter)

logger = logging.getLogger('streamcorpus_pipeline')
logger.setLevel(_log_level)
logger.handlers = []
logger.addHandler(ch)

kazoo_log = logging.getLogger('kazoo')
#kazoo_log.setLevel(logging.DEBUG)
kazoo_log.addHandler(ch)


_known_loggers = [logger, kazoo_log]


def configure_logger(xlogger):
    '''
    for instance from loggign.getLogger(), configure it.
    '''
    xlogger.handlers = []
    xlogger.addHandler(ch)
    xlogger.setLevel(_log_level)
    _known_loggers.append(xlogger)


def reset_log_level( log_level ):
    '''
    set logging framework to log_level

    initial default is DEBUG
    '''
    global _log_level
    _log_level = log_level
    for l in _known_loggers:
        l.setLevel(log_level)
