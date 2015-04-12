"""Read spinn3r protostreams directly into the pipeline.

Purpose
=======

Read a spinn3r.com protostream file, the spinn3r.com Web API, or
continuously fetch spinn3r.com streams as a reader.

In your streamcorpus-pipeline YAML configuration, specify::

    reader: from_spinn3r_feed

Give a local file name containing a fetched spinn3r protostream file
as the input to the pipeline.

Implementation details
======================

The feed file itself consists of a sequence of length-prefixed chunks,
with alternating type and data chunks.  The file continues until a
"type" chunk specifies "END".  Each of the data chunks should produce
one StreamItem into the rest of the pipeline.

`from_spinn3r_feed` is the main reader.  It is exported to the
pipeline configuration in `streamcorpus_pipeline.stages`.

The spinn3r data can be injected externally.  Create a dictionary
mapping keys to the protostream data from spinn3r, cause it to be
passed to the stage constructor, set a configuration parameter
``use_prefetched: true``, and invoke the pipeline with the provided
key as the input filename.  The key can be any valid string.

`ProtoStreamReader` will read chunks of the spinn3r document, typically
in the spinn3rApi_pb2.Entry Google protobuf format.

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.

"""

from __future__ import absolute_import

import itertools
import json
import logging
from StringIO import StringIO
import zlib

from google.protobuf.internal.decoder import _DecodeVarint

import streamcorpus
from streamcorpus_pipeline._exceptions import ConfigurationError
from streamcorpus_pipeline._spinn3r.protoStream_pb2 \
    import ProtoStreamDelimiter, ProtoStreamHeader
from streamcorpus_pipeline._spinn3r.spinn3rApi_pb2 import Entry
from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)


class ProtoStreamReader(object):
    """Reader object for spinn3r "protostream" files.

    These files are produced by spinn3r's external API.  The actual
    files consist of length-prefixed chunks, where the length is in
    protobuf varint format.  The actual file consists of a header,
    then alternating "delimiter" and data chunks.  The file ends when
    a delimiter chunk has an "end" flag.

    Create this with a file-like object, and then iterate through
    the produced chunks.  By default it will produce spinn3rApi_pb2.Entry
    objects out.  For example:

    >>> with open(filename, 'rb') as f:
    ...   reader = ProtoStreamReader(f)
    ...   for entry in reader:
    ...     ...

    The `header` property will contain the
    protoStream_pb2.ProtoStreamHeader object from the file header.

    """

    def __init__(self, f, entry_type=Entry):
        """Create a new ProtoStreamReader.

        ``f``
          file-like object to read
        ``entry_type``
          type of objects in this feed

        """
        super(ProtoStreamReader, self).__init__()
        self.f = f
        self._prefix = ''
        self._header = None
        self._entry_type = entry_type

    def _unread(self, b):
        """Push byte array 'b' on to the front of the prefix list; this
        undoes part of a _read().  This bytes will be the next to read."""
        self._prefix = b + self._prefix

    def _read(self, n):
        """Read (up to) 'n' bytes from the underlying file.  If any bytes
        have been pushed in with _unread() those are returned first."""
        if n <= len(self._prefix):
            # the read can be fulfilled entirely from the prefix
            result = self._prefix[:n]
            self._prefix = self._prefix[n:]
            return result
        # otherwise we need to read some
        n -= len(self._prefix)
        result = self._prefix + self.f.read(n)
        self._prefix = ""
        return result

    def _read_varint(self):
        """Read exactly a varint out of the underlying file."""
        buf = self._read(8)
        (n, l) = _DecodeVarint(buf, 0)
        self._unread(buf[l:])
        return n

    def _read_block(self):
        """Read a length from the file, then read that many bytes.
        Return the read block."""
        l = self._read_varint()
        return self._read(l)

    def _read_a(self, cls):
        """Read some protobuf-encoded object stored in a single block
        out of the file."""
        o = cls()
        o.ParseFromString(self._read_block())
        return o

    def _bootstrap(self):
        """Read the header block out of the underlying file.
        After this (and forevermore afterwards) the file will point
        at the start of a ProtoStreamDelimiter block."""
        self._header = self._read_a(ProtoStreamHeader)

    @property
    def header(self):
        if self._header is None:
            self._bootstrap()
        return self._header

    def __iter__(self):
        """Iterate through the objects in the file."""
        if self._header is None:
            self._bootstrap()
        while True:
            delim = self._read_a(ProtoStreamDelimiter)
            if delim.delimiter_type == ProtoStreamDelimiter.END:
                return
            assert delim.delimiter_type == ProtoStreamDelimiter.ENTRY
            yield self._read_a(self._entry_type)


class from_spinn3r_feed(Configured):
    """streamcorpus-pipeline reader for spinn3r.com feeds.

    Accepted configuration items include:

    ``use_prefetched``
      If set to a boolean value, always/never use prefetched data
      (default: unset; use prefetched data if present)
    ``publisher_type``
      If set, only return stream items whose publisher type in the
      spinn3r feed exactly matches this string

    A dictionary from URL to prefetched data can be passed as a
    parameter to the stage constructor.  If `use_prefetched` is
    :const:`True` then all input strings must be present in the
    prefetch dictionary, and this stage never makes an outgoing
    network connection.  If `use_prefetched` is :const:`False` then
    the prefetch dictionary is ignored.  Otherwise if an input string
    is present in the prefetch dictionary, then the prefetched data is
    used, and if not, it is fetched from the network.

    """
    config_name = 'from_spinn3r_feed'

    def __init__(self, config={}, prefetched=None):
        super(from_spinn3r_feed, self).__init__(config)
        if prefetched is None:
            prefetched = {}
        self.prefetched = prefetched

    def __call__(self, i_str):
        # Do we have prefetched data for i_str?  Can/should/must we
        # use it?
        if ((self.config.get('use_prefetched', False) and
             i_str not in self.prefetched)):
            raise ConfigurationError('from_spinn3r_feed "use_prefetched" '
                                     'is set, but prefetched content is '
                                     'missing')
        if ((self.config.get('use_prefetched', True) and
             i_str in self.prefetched)):
            logger.debug('using prefetched content for {0}'.format(i_str))
            stream = StringIO(self.prefetched[i_str])
        else:
            logger.debug('getting local content from {0}'.format(i_str))
            stream = open(i_str, 'rb')

        try:
            count = 0
            # spinn3r publisher type == streamitem source
            source = self.config.get('publisher_type')
            for si in _make_stream_items(stream):
                if source is not None and si.source != source:
                    continue
                count += 1
                yield si
            logger.info('produced {0} stream items'.format(count))
        finally:
            stream.close()


def _generate_stream_items(data):
    return _make_stream_items(StringIO(data))


def _make_stream_items(f):
    """Given a spinn3r feed, produce a sequence of valid StreamItems.

    Because of goopy Python interactions, you probably need to call
    this and re-yield its results, as

    >>> with open(filename, 'rb') as f:
    ...   for si in _make_stream_items(f):
    ...     yield si

    """
    reader = ProtoStreamReader(f)
    return itertools.ifilter(
        lambda x: x is not None,
        itertools.imap(_make_stream_item, reader))


def _make_stream_item(entry):
    """Given a single spinn3r feed entry, produce a single StreamItem.

    Returns 'None' if a complete item can't be constructed.

    """
    # get standard metadata, assuming it's present...
    if not hasattr(entry, 'permalink_entry'):
        return None
    pe = entry.permalink_entry

    # ...and create a streamitem...
    si = streamcorpus.make_stream_item(
        pe.date_found[:-1] + '.0Z',
        pe.canonical_link.href.encode('utf8'))
    if not si.stream_time:
        logger.debug('failed to generate stream_time from {0!r}'
                     .format(pe.date_found))
        return None
    if not si.abs_url:
        logger.debug('failed to generate abs_url from {0!r}'
                     .format(pe.canonical_link.href))
        return None

    # ...filling in the actual data
    si.body = _make_content_item(
        pe.content,
        alternate_data=entry.feed_entry.content.data)
    if not si.body:
        return None
    if not si.body.raw:
        return None

    if pe.content_extract.data:
        si.other_content['extract'] = _make_content_item(pe.content_extract)
    si.other_content['title'] = streamcorpus.ContentItem(
        raw=pe.title.encode('utf8'),
        media_type=pe.content_extract.mime_type,
        encoding='UTF-8')
    si.other_content['feed_entry_title'] = streamcorpus.ContentItem(
        raw=entry.feed_entry.title.encode('utf8'),
        media_type=entry.feed_entry.content.mime_type,
        encoding='UTF-8')
    if entry.feed_entry.content.data:
        si.other_content['feed_entry'] = _make_content_item(
            entry.feed_entry.content)
    si.source_metadata['lang'] = pe.lang[0].code
    si.source_metadata['author'] = json.dumps(
        dict(
            name=pe.author[0].name,
            email=pe.author[0].email,
            link=pe.author[0].link[0].href,
        )
    )
    si.source = entry.source.publisher_type
    return si


def _make_content_item(node, mime_type=None, alternate_data=None):
    """Create a ContentItem from a node in the spinn3r data tree.

    The ContentItem is created with raw data set to ``node.data``,
    decompressed if the node's encoding is 'zlib', and UTF-8
    normalized, with a MIME type from ``node.mime_type``.

    ``node``
      the actual node from the spinn3r protobuf data
    ``mime_type``
      string MIME type to use (defaults to ``node.mime_type``)
    ``alternate_data``
      alternate (compressed) data to use, if ``node.data`` is missing
      or can't be decompressed

    """
    raw = node.data
    if getattr(node, 'encoding', None) == 'zlib':
        try:
            raw = zlib.decompress(node.data)
        except Exception, exc:
            if alternate_data is not None:
                try:
                    raw = zlib.decompress(alternate_data)
                except Exception:
                    raise exc  # the original exception
                else:
                    raise
    if mime_type is None:
        mime_type = node.mime_type
    raw = raw.decode('utf8').encode('utf8')
    return streamcorpus.ContentItem(raw=raw, media_type=mime_type)
