'''
incremental transform that detects language and stores it in
StreamItem.body.language

This software is released under an MIT/X11 open source license.

Copyright 2012-2014 Diffeo, Inc.
'''

from __future__ import absolute_import
import cld
import logging
from streamcorpus import Language
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

class language(Configured):
    '''Guess at a language from ``body``.

    This always adds a ``language`` annotation to the body, but if
    the body does not have a raw part or the language cannot be reliably
    detected, it may be empty.

    This attempts to detect on ``body.clean_html`` if present, and
    then ``body.clean_visible`` if present, and then falls back to
    attempting to use ``body.encoding`` to decode the ``body.raw`` and
    tries to detect language from the output of that, and finally, it
    attempts to detect language on undecoded ``body.raw``.

    This has no configuration options.

    '''
    config_name = 'language'
    def __call__(self, si, context):
        if not si.body:
            return si

        b = si.body
        is_reliable = False
        code = None
        name = None

        if ((not is_reliable) or code == 'xxx') and b.clean_html:
            name, code, is_reliable, num_text_bytes, details = \
                cld.detect(b.clean_html)

        if ((not is_reliable) or code == 'xxx') and b.clean_visible:
            name, code, is_reliable, num_text_bytes, details = \
                cld.detect(b.clean_visible)

        if ((not is_reliable) or code == 'xxx') and b.encoding and b.raw:
            raw = None
            try:
                raw = b.raw.decode(b.encoding, 'ignore')\
                                 .encode('utf8', 'ignore')
            except Exception, exc:
                logger.warn('failed to decode body.raw using body.encoding=%r',
                            b.encoding, exc_info=True)
            if raw:
                name, code, is_reliable, num_text_bytes, details = cld.detect(raw)

        if ((not is_reliable) or code == 'xxx') and b.raw:
            name, code, is_reliable, num_text_bytes, details = cld.detect(b.raw)

        # always create `body.language`
        b.language = Language(code='', name='')
        if is_reliable and code != 'xxx':
            b.language.code = code
            b.language.name = name

        if 'force' in self.config:
            b.language.code = self.config['force'].get('code')
            b.language.name = self.config['force'].get('name')

        return si
