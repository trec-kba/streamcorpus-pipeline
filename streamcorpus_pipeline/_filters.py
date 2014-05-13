'''
Incremental transforms that only pass specific StreamItems.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import
from itertools import imap, chain
import logging
import re
from urlparse import urlparse

from streamcorpus_pipeline.stages import Configured


logger = logging.getLogger(__name__)


class debug_filter(Configured):
    '''Remove all stream items except specified ones.

    This is only needed to debug troublesome configurations if there
    are problems with specific stream items.  It has one
    configuration item:

    .. code-block:: yaml

        accept_stream_ids:
          - 1360783522-ebf059defa41b2812c1f969f28cdb45e

    Only stream IDs in the list are passed on, all others are dropped.
    Defaults to an empty list (drop all stream items).

    '''
    config_name = 'debug_filter'
    default_config = { 'accept_stream_ids': [] }
    def __call__(self, si, context):
        if si.stream_id in self.config['accept_stream_ids']:
            return si
        else:
            return None

class exclusion_filter(Configured):
    '''Remove specific stream items.

    This is not usually needed, but if specific stream items have been
    identified to cause problems with a tagger or other stages, this
    can remove them before they run.  It has one configuration item:

    .. code-block:: yaml

        excluded_stream_ids:
          - 1360783522-ebf059defa41b2812c1f969f28cdb45e

    Stream IDs in the list are dropped, all others are passed on
    unmodified.  Defaults to an empty list (don't drop any stream
    items).

    '''
    config_name = 'exclusion_filter'
    default_config = { 'excluded_stream_ids': [] }
    def __call__(self, si, context):
        if si.stream_id in self.config['excluded_stream_ids']:
            return None
        else:
            return si

class filter_languages(Configured):
    '''Remove stream items that aren't in specific languages.

    This looks at the :attr:`~streamcorpus.StreamItem.body`
    :attr:`~streamcorpus.ContentItem.language` field.  It has
    two configuration options:

    .. code-block:: yaml

        filter_languages:
          allow_null_language: True
          included_language_codes: [en]

    If the language field is empty, the stream item is dropped unless
    `allow_null_language` is true (default value).  Otherwise, the
    stream item is dropped unless its language code is one of the
    `included_language_codes` values (defaults to empty list).

    '''
    config_name = 'filter_languages'
    default_config = { 'allow_null_language': True,
                       'included_language_codes': [] }
    def __call__(self, si, context):
        if not si.body.language and self.config['allow_null_language']:
            return si
        if si.body.language.code in self.config['included_language_codes']:
            return si
        ## otherwise return None, which excludes the language
        return None

class remove_raw(Configured):
    '''Remove the raw form of the content.

    While the :attr:`~streamcorpus.StreamItem.body`
    :attr:`~streamcorpus.ContentItem.raw` form is important as inputs
    to create the "clean" forms, it may be bulky and not useful later.
    This strips the raw form, replacing it with an empty string.  It
    has one configuration option:

    .. code-block:: yaml

        remove_raw:
          if_clean_visible_remove_raw: True

    If `if_clean_visible_remove_raw` is set (defaults to false),
    the raw form is only removed if the clean\_visible form is
    available; otherwise it is always removed.

    '''
    config_name = 'remove_raw'
    default_config = { 'if_clean_visible_remove_raw': False }
    def __call__(self, si, context):
        if self.config['if_clean_visible_remove_raw']:
            if si.body.clean_visible:
                si.body.raw = ''
            ## otherwise leave body.raw unchanged
        else:
            si.body.raw = ''

        return si


def domain_name_cleanse(raw_string):
    '''extract a lower-case, no-slashes domain name from a raw string
    that might be a URL
    '''
    try:
        parts = urlparse(raw_string)
        domain = parts.netloc.split(':')[0]
    except:
        domain = ''
    if not domain:
        domain = raw_string
    if not domain:
        return ''
    domain = re.sub('\/', '', domain.strip().lower())
    return domain

def domain_name_left_cuts(domain):
    '''returns a list of strings created by splitting the domain on
    '.' and successively cutting off the left most portion
    '''
    cuts = []
    if domain:
        parts = domain.split('.')
        for i in range(len(parts)):
            cuts.append( '.'.join(parts[i:]))
    return cuts

class filter_domains(Configured):
    '''Remove stream items that are not from a specific domain by
    inspecting first :attr:`~StreamItem.abs_url` and then
    :attr:`~StreamItem.schost`

    .. code-block:: yaml

        filter_domains:
          include_domains: [example.com]
          include_domains_path:
          - path-to-file-with-one-domain-per-line.txt
          - path-to-file-with-one-domain-per-line2.txt

    '''
    config_name = 'filter_domains'
    default_config = { 'include_domains': [] }
    def __init__(self, config, *args, **kwargs):
        super(filter_domains, self).__init__(config, *args, **kwargs)

        ## cleanse the input domains lists
        self.domains = set()

        include_domains = self.config.get('include_domains', [])
        map(self.domains.add, imap(domain_name_cleanse, include_domains))

        include_domains_path = self.config.get('include_domains_path', [])
        if not isinstance(include_domains_path, list):
            include_domains_path = [include_domains_path]
        map(self.domains.add, imap(
                domain_name_cleanse, 
                chain(*imap(open, include_domains_path))))

        logger.info('filter_domains configured with %d domain names',
                    len(self.domains))
        logger.info('filter_domains.domains = %r', self.domains)

    def __call__(self, si, context=None):
        url_things = [si.abs_url, si.schost]
        for thing in url_things:
            if not thing:
                continue
            for cut in domain_name_left_cuts(domain_name_cleanse(thing)):
                if cut in self.domains:
                    logger.info('found: %r', cut)
                    return si

        ## otherwise return None, which excludes the stream item
        logger.debug('rejecting: %r %r', si.schost, si.abs_url)
        return None
