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


def configure_logger(name):
    '''
    for instance from logging.getLogger(), configure it.
    '''
    xlogger = logging.getLogger(name)
    xlogger.handlers = []
    for handler in _known_handlers:
        xlogger.addHandler(handler)
    xlogger.setLevel(_log_level)
    global _known_loggers
    _known_loggers.append(xlogger)
    return xlogger

def reset_log_level( log_level ):
    '''
    set logging framework to log_level

    initial default is DEBUG
    '''
    global _log_level
    _log_level = log_level
    for l in _known_loggers:
        l.setLevel(log_level)


_log_level = logging.DEBUG

ch = logging.StreamHandler()
formatter = FixedWidthFormatter('%(asctime)s pid=%(process)d %(fixed_width_filename_lineno)s %(fixed_width_levelname)s %(message)s')
ch.setLevel(_log_level)
ch.setFormatter(formatter)

_known_handlers = [ch]
_known_loggers = []

configure_logger('streamcorpus')
logger = configure_logger('streamcorpus_pipeline')
