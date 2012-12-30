#!/usr/bin/env python
'''
Provides a data transformation pipeline for expanding the data in
StreamItem instances from streamcorpus.Chunk files.  Also includes a
set of standard transforms that can be used in a Pipeline.

See help(Pipeline) for details on configuring a Pipeline.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''

from ._pipeline import Pipeline
from ._clean_html import clean_html
from ._clean_visible import clean_visible
from ._hyperlink_labels import hyperlink_labels
from ._lingpipe import lingpipe
