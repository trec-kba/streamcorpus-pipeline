#!/usr/bin/env python
'''
Provides a data transformation pipeline for expanding the data in
StreamItem instances from streamcorpus.Chunk files.  Also includes a
set of standard transforms that can be used in a Pipeline.

See help(Pipeline) for details on configuring a Pipeline.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
__all__ = ['Pipeline']
from ._pipeline import Pipeline
