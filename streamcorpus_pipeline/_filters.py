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

from backports import lzma

from streamcorpus_pipeline.stages import Configured


logger = logging.getLogger(__name__)


class dump_stream_id_abs_url(Configured):
    '''print :attr:`~streamcorpus.StreamItem.stream_id`,
    :attr:`~streamcorpus.StreamItem.abs_url` strings to stdout and do
    not pass any :class:`~streamcorpus.StreamItems` down the pipeline

    '''
    config_name = 'dump_abs_url'
    default_config = {}
    def __call__(self, si, context):
        print si.stream_id, si.abs_url
        return None


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


class id_filter(Configured):
    '''Include or exclude specific stream items by doc_id or stream_id

    If specific stream items have been identified, e.g. as causing
    problems with a tagger or other stages, then this can remove them
    before they run.  

    .. code-block:: yaml

      id_filter:
        excluded_stream_ids_path: file-of-one-stream-id-per-line.txt.xz
        excluded_stream_ids:
          - 1360783522-ebf059defa41b2812c1f969f28cdb45e
        included_stream_ids_path: file-of-one-stream-id-per-line.txt.xz
        included_stream_ids:
          - 1360783522-ebf059defa41b2812c1f969f28cdb45e
        included_doc_ids_path: file-of-one-doc-id-per-line.txt.xz
        included_doc_ids:
          - ebf059defa41b2812c1f969f28cdb45e
        excluded_doc_ids_path: file-of-one-doc-id-per-line.txt.xz
        excluded_doc_ids:
          - ebf059defa41b2812c1f969f28cdb45e


    Stream IDs in the `excluded_stream_ids` list are dropped, all
    others are passed on unmodified.  Defaults to an empty list (don't
    drop any stream items).

    In addition to listing Stream IDs in the yaml itself, you can pass
    `excluded_stream_ids_path` with one Stream ID per line.  This file
    can be optionally be xz-compressed.

    '''
    config_name = 'id_filter'
    default_config = { 
        'excluded_stream_ids': [],
        'included_stream_ids': [],
        'excluded_doc_ids': [],
        'included_doc_ids': [],
        }

    def __init__(self, *args, **kwargs):
        super(id_filter, self).__init__(*args, **kwargs)
        self.included_doc_ids = set()
        self.excluded_doc_ids = set()
        self.included_stream_ids = set()
        self.excluded_stream_ids = set()
        for mode in ['excluded', 'included']:
            for id_type in ['stream_ids', 'doc_ids']:
                self.configure(mode, id_type)

    def configure(self, mode, id_type):
        mode_type = '%s_%s' % (mode, id_type)
        data = set(self.config.get(mode_type, []))
        path = self.config.get(mode_type + '_path')
        if path:
            if path.endswith('.xz'):
                fh = lzma.open(path)
            else:
                fh = open(path)
            map(data.add, fh.read().splitlines())
        setattr(self, mode_type, data)
        logger.info('finished loading %d %s to %s', len(data), id_type, mode)

    def __call__(self, si, context):
        if si.stream_id in self.excluded_stream_ids:
            return None
        if si.doc_id in self.excluded_stream_ids:
            return None

        if self.included_stream_ids or self.included_doc_ids:
            if si.stream_id in self.included_stream_ids:
                return si
            if si.doc_id in self.included_doc_ids:
                return si
        else:
            ## not filtering by inclusion
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


class filter_tagger_ids(Configured):
    '''Remove stream items that lack a StreamItem.body.sentences entry
    from specified tagger_id(s)

    This looks at the :attr:`~streamcorpus.StreamItem.body.sentences`
    field.  It has two configuration options:

    .. code-block:: yaml

        filter_tagger_ids:
          tagger_ids_to_keep:
            - serif

    If the none of the tagger_ids_to_keep are in
    :attr:`~streamcorpus.StreamItem.body.sentences`, the stream item
    is dropped.  If one of the required tagger_ids is present, but
    points to an empty list, then it is also dropped.

    '''
    config_name = 'filter_tagger_ids'
    default_config = { 'tagger_ids_to_keep': [] }
    def __call__(self, si, context):
        if not si.body or not si.body.sentences:
            return None
        for tagger_id in self.config['tagger_ids_to_keep']:
            sentences = si.body.sentences.get(tagger_id)
            if sentences:
                return si
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


class replace_raw(Configured):
    '''Replace the `raw` form of the content with `clean_html`, or delete
    the StreamItem if it lacks `clean_html`.

    While the :attr:`~streamcorpus.StreamItem.body`
    :attr:`~streamcorpus.ContentItem.raw` form is important as inputs
    to create the "clean" forms, it may be bulky and not useful later.
    This strips the raw form, replacing it with clean_html.

    '''
    config_name = 'replace_raw'
    default_config = {}
    def __call__(self, si, context=None):
        if si.body.clean_html:
            si.body.raw = si.body.clean_html
            return si
        else:
            return None # delete


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
    :attr:`~StreamItem.schost`.  domain name strings are cleansed.

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


class filter_domains_substrings(Configured):
    '''Remove stream items that are not from a domain whose name contains
    one of the specified strings by inspecting first
    :attr:`~StreamItem.abs_url` and then :attr:`~StreamItem.schost`.
    Substrings are used exactly as provided without cleansing.

    .. code-block:: yaml

        filter_domains_matching:
          include_substrings: [example]
          include_substrings_path:
          - path-to-file-with-one-substring-per-line.txt
          - path-to-file-with-one-substring-per-line2.txt

    '''
    config_name = 'filter_domains_substrings'
    default_config = { 'include_substrings': [] }
    def __init__(self, config, *args, **kwargs):
        super(filter_domains_substrings, self).__init__(config, *args, **kwargs)

        ## cleanse the input substrings lists
        self.substrings = set()

        include_substrings = self.config.get('include_substrings', [])
        map(self.substrings.add, include_substrings)

        include_substrings_path = self.config.get('include_substrings_path', [])
        if not isinstance(include_substrings_path, list):
            include_substrings_path = [include_substrings_path]
        map(self.substrings.add, 
            imap(lambda s: s.strip(), 
                 chain(*imap(open, include_substrings_path))))

        logger.info('filter_substrings configured with %d domain names',
                    len(self.substrings))
        logger.info('filter_domans_substrings.substrings = %r', self.substrings)

    def __call__(self, si, context=None):
        url_things = [si.abs_url, si.schost]
        for thing in url_things:
            if not thing:
                continue
            for substr in self.substrings:
                if substr in thing:
                    logger.info('found: %r in %r', substr, thing)
                    return si

        ## otherwise return None, which excludes the stream item
        logger.debug('rejecting: %r %r', si.schost, si.abs_url)
        return None
