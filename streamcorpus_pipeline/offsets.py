from __future__ import absolute_import, division, print_function

from HTMLParser import HTMLParser
from itertools import izip
import logging

from streamcorpus import InvalidXpathError, Offset, OffsetType, XpathRange


logger = logging.getLogger(__name__)


# In HTML5, void elements are technically distinct from self-closing elements.
# Void elements have a "start" but no end tag, while self-closing elements
# have cause both the "start" and "end" events to fire.
#
# In our HTML parser, we need to be careful with void element handling,
# otherwise our state gets corrupt (because we cannot assume every start
# tag has an end tag).
VOID_ELEMENTS = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input',
                 'keygen', 'link', 'meta', 'param', 'source', 'track', 'wbr'}


# To be written transform.
class xpath_offsets(object):
    config_name = 'xpath_offsets'
    default_config = {}

    def __init__(self, config):
        self.config = config

    def __call__(self, si, context=None):
        if not si.body or not si.body.clean_html or not si.body.clean_visible:
            logger.warning('stream item %s: has no clean_{html,visible}, '
                           'so we will not generate Xpath offsets',
                           si.stream_id)
            return si
        add_xpaths_to_stream_item(si)
        try:
            stream_item_roundtrip_xpaths(si, quick=True)
        except XpathMismatchError:
            logger.warning('stream item %s: Failed xpath roundtrip test, '
                           'dropping', si.stream_id, exc_info=True)
            return None
        return si


class XpathTextCollector(HTMLParser):
    '''Collects an HTML parse into an xpath.

    An instance of this class *statefully* constructs an xpath
    that corresponds to the HTML that has been parsed thus far.
    Basic usage of this class is to:

        1. Create the parser.
        2. Feed it some HTML.
        3. Ask for the xpath pointing to the location at which
           the aforementioned HTML ended.
        4. Go back to step 2 until all HTML has been consumed.

    This process is useful when you have a contiguous sequence
    of HTML fragments that, when concatenated, form a single
    HTML document. In particular, this enables one to compute
    *addresses* for each fragment.

    To that end, the xpaths generated by this class have a few
    very important properties:

        1. All xpaths *uniquely* identify a *single* text node
           in the HTML document.
        2. The offsets generated are actually of the form
           `(xpath, char offset)`, where the character offset
           indicates where the text starts in the text node
           addressed by `xpath`.
        3. The combination of two offsets forms a *range*, which
           is isomorphic to `Range` objects found in Javascript's
           standard library (which are used to represent user
           selections of text).
    '''
    def __init__(self):
        HTMLParser.__init__(self)  # old-style class :-/

        # The current depth. Incremented when a tag is entered and decremented
        # when a tag is exited.
        self.depth = 0
        # The index at which the most recent data node ends. The index is
        # relative to the data node. (It is reset on every start tag.)
        self.data_start = 0
        # depth |--> {'path': [tag | data], 'tags': tag |--> count}
        #   where `data` is represented via `None`.
        #
        # This is used to build the indices of each element in the xpath.
        # Note that data nodes are also tracked, which are used to compute
        # the data node indices.
        #
        # e.g., `[None, None, p, None]` means that `handle_data` was fired
        # twice, then `handle_starttag` and then `handle_data` again. The
        # last `handle_data` corresponds to the start of the *second* text
        # node in the context's parent node. (See `self.text_index`.)
        #
        # This is all tracked *per depth*. When the parser leaves a depth,
        # that depth's stack is popped until it's empty.
        #
        # Finally, this representation enables us to pick out the current
        # xpath by peeking at the top of each stack from the smallest to
        # largest depth.
        #
        # (We could compute the indices from just `path` by counting elements,
        # but this is a perf problem. So we track the counts explicitly in
        # a `tags` dict.)
        self.depth_stack = {}
        self.init_depth(0)

    # This is the one public method!
    def xpath_offset(self):
        '''Returns a tuple of ``(xpath, character offset)``.

        The ``xpath`` returned *uniquely* identifies the end of the
        text node most recently inserted. The character offsets
        indicates where the text inside the node ends. (When the text
        node is empty, the offset returned is `0`.)
        '''
        datai = self.text_index()
        xpath = '/' + '/'.join(self.xpath_pieces()) + ('/text()[%d]' % datai)
        return (uni(xpath), self.data_start)

    # These help with xpath generation.
    def xpath_pieces(self):
        '''A generator for a canonical xpath to the current node.

        This does not include the text node.

        The generator yields strings, where each string is a component
        in the xpath. Joining them with ``/`` is valid, but it will not
        be rooted.
        '''
        for d in xrange(self.depth):
            tag = self.depth_stack[d]['path'][-1]
            yield '%s[%d]' % (tag, self.depth_stack[d]['tags'][tag])

    def text_index(self):
        '''Returns the one-based index of the current text node.'''
        i = 1
        last_is_data = False
        for v in self.depth_stack[self.depth]['path']:
            if v is None and not last_is_data:
                last_is_data = True
            elif v is not None and last_is_data:
                i += 1
                last_is_data = False
        return i

    # Some hacks to track whether the parser moved to the next state.
    def feed(self, s):
        # print('FEED', s)
        HTMLParser.feed(self, s[0:len(s)-1])
        self.made_progress = False
        HTMLParser.feed(self, s[len(s)-1:len(s)])

    def progressor(meth):
        def _(self, *args, **kwargs):
            # print(meth.__name__, args[0])
            self.made_progress = True
            return meth(self, *args, **kwargs)
        return _

    # Helpers.
    def add_element(self, depth, tag):
        d = self.depth_stack[depth]
        d['path'].append(tag)
        if tag not in d['tags']:
            d['tags'][tag] = 1
        else:
            d['tags'][tag] += 1

    def remove_element(self, depth, tag):
        self.depth_stack[depth]['path'].pop()
        self.depth_stack[depth]['tags'][tag] -= 1

    def init_depth(self, depth):
        self.depth_stack[depth] = {'path': [], 'tags': {}}

    # Satisfy the `HTMLParser` interface.
    @progressor
    def handle_starttag(self, tag, attrs):
        self.data_start = 0
        self.add_element(self.depth, tag)
        if tag in VOID_ELEMENTS:
            # Void elements are special. We effectively want to ignore them
            # completely, although we do need to make sure they affect node
            # indexing.
            return

        self.depth += 1
        self.init_depth(self.depth)

    @progressor
    def handle_endtag(self, tag):
        d = self.depth_stack
        if tag in VOID_ELEMENTS:
            # We don't "descend" into void elements, so we can just ignore
            # them completely.
            return
        d.pop(self.depth)
        self.data_start = 0
        self.depth -= 1
        assert d[self.depth]['path'][-1] == tag, \
                '%s not at end of %r' % (tag, d[self.depth]['path'])

    @progressor
    def handle_startendtag(self, tag, attrs):
        # This is *only* called for self-closing elements, e.g., `<br />`.
        # It is NOT called for void elements, e.g., `<br>`.
        self.add_element(self.depth, tag)
        self.data_start = 0

    # The following methods handle data. They are responsible for recording
    # enough state such that:
    #
    #   1) The index of the current text node can be computed.
    #   2) The character offset of the current position in the current
    #      text node can be computed.
    #
    # (1) is achieved by adding `None` elements to the xpath stack. These
    # `None` elements are collapsed in the `text_index` method.
    #
    # (2) is achieved by keeping track of the length of data (in terms of
    # Unicode codepoints) in the current text node. This length is reset
    # whenever a new tag is opened.

    @progressor
    def handle_data(self, data):
        # There is a bug in `HTMLParser` where the data yielded can be
        # hard-coded in certain state transitions. This means it could yield
        # data that is `bytes` even though we gave it a Unicode string. ---AG
        data = uni(data)
        self.add_element(self.depth, None)
        self.data_start += len(data)

    @progressor
    def handle_entityref(self, name):
        self.add_element(self.depth, None)
        # The `2` is for the `&` and `;` in, e.g., `&amp;`.
        self.data_start += 2 + len(name)

    @progressor
    def handle_charref(self, name):
        self.add_element(self.depth, None)
        # The `3` is for the `&`, `#` and `;` in, e.g., `&#36;`.
        # This also applies to hex codes as `name` will contain the
        # preceding `x`.
        self.data_start += 3 + len(name)

    # TODO: There are some other callbacks defined in the `HTMLParser`
    # interface. We probably need to do something with them for complete
    # correctness. (If they can't be handled for whatever reason, simply
    # declare those states as not making progress---`char_offsets_to_xpaths`
    # will handle it gracefully by not computing xpaths.) ---AG


class XpathMismatchError(Exception):
    '''Raised when an Xpath offset is wrong.

    This occurs when slicing ``clean_html`` with xpaths does not
    produce the exact same string as slicing ``clean_visible``
    with character offsets.

    This class has four instance variables:

    * ``clean_html`` and ``clean_visible`` are a ``unicode`` strings.
    * ``xp_range`` is a :class:`streamcorpus.XpathRange`.
    * ``char_range`` is a ``(int, int)`` (half-open character range).
    '''
    def __init__(self, html, cleanvis, xp_range, char_range):
        self.clean_html = html
        self.clean_visible = cleanvis
        self.xp_range = xp_range
        self.char_range = char_range


def add_xpaths_to_stream_item(si):
    '''Mutably tag tokens with xpath offsets.

    Given some stream item, this will tag all tokens from all taggings
    in the document that contain character offsets. Note that some
    tokens may not have computable xpath offsets, so an xpath offset
    for those tokens will not be set. (See the documentation and
    comments for ``char_offsets_to_xpaths`` for what it means for a
    token to have a computable xpath.)

    If a token can have its xpath offset computed, it is added to its
    set of offsets with a ``OffsetType.XPATH_CHARS`` key.
    '''
    def sentences_to_xpaths(sentences):
        tokens = sentences_to_char_tokens(sentences)
        offsets = char_tokens_to_char_offsets(tokens)
        return char_offsets_to_xpaths(html, offsets)

    def xprange_to_offset(xprange):
        return Offset(type=OffsetType.XPATH_CHARS,
                      first=xprange.start_offset, length=0,
                      xpath=xprange.start_xpath,
                      content_form='clean_html', value=None,
                      xpath_end=xprange.end_xpath,
                      xpath_end_offset=xprange.end_offset)

    html = unicode(si.body.clean_html, 'utf-8')
    for sentences in si.body.sentences.itervalues():
        tokens = sentences_to_char_tokens(sentences)
        for token, xprange in izip(tokens, sentences_to_xpaths(sentences)):
            if xprange is None:
                continue
            offset = xprange_to_offset(xprange)
            token.offsets[OffsetType.XPATH_CHARS] = offset


def sentences_to_char_tokens(si_sentences):
    '''Convert stream item sentences to character ``Offset``s.'''
    for sentence in si_sentences:
        for token in sentence.tokens:
            if OffsetType.CHARS in token.offsets:
                yield token


def char_tokens_to_char_offsets(si_tokens):
    '''Convert character ``Offset``s to character ranges.'''
    for token in si_tokens:
        offset = token.offsets[OffsetType.CHARS]
        yield offset.first, offset.first + offset.length


def char_offsets_to_xpaths(html, char_offsets):
    '''Converts HTML and a sequence of char offsets to xpath offsets.

    Returns a generator of :class:`streamcorpus.XpathRange` objects
    in correspondences with the sequence of ``char_offsets`` given.
    Namely, each ``XpathRange`` should address precisely the same text
    as that ``char_offsets`` (sans the HTML).

    Depending on how ``char_offsets`` was tokenized, it's possible that
    some tokens cannot have their xpaths generated reliably. In this
    case, a ``None`` value is yielded instead of a ``XpathRange``.

    ``char_offsets`` must be a sorted and non-overlapping sequence of
    character ranges. They do not have to be contiguous.
    '''
    html = uni(html)
    parser = XpathTextCollector()
    prev_end = 0
    prev_progress = True
    for start, end in char_offsets:
        if start == end:
            # Zero length tokens shall have no quarter!
            # Note that this is a special case. If we let zero-length tokens
            # be handled normally, then it will be recorded as if the parser
            # did not make any progress. But of course, there is no progress
            # to be had!
            yield None
            continue
        # If we didn't make any progress on the previous token, then we'll
        # need to try and make progress before we can start tracking offsets
        # again. Otherwise the parser will report incorrect offset info.
        #
        # (The parser can fail to make progress when tokens are split at
        # weird boundaries, e.g., `&amp` followed by `;`. The parser won't
        # make progress after `&amp` but will once `;` is given.)
        #
        # Here, we feed the parser one character at a time between where the
        # last token ended and where the next token will start. In most cases,
        # this will be enough to nudge the parser along. Once done, we can pick
        # up where we left off and start handing out offsets again.
        #
        # If this still doesn't let us make progress, then we'll have to skip
        # this token too.
        if not prev_progress:
            for i in xrange(prev_end, start):
                parser.feed(html[i])
                prev_end += 1
                if parser.made_progress:
                    break
            if not parser.made_progress:
                yield None
                continue
        # Hand the parser everything from the end of the last token to the
        # start of this one. Then ask for the Xpath, which should be at the
        # start of `char_offsets`.
        if prev_end < start:
            parser.feed(html[prev_end:start])
            if not parser.made_progress:
                parser.feed(html[start:end])
                prev_progress = parser.made_progress
                prev_end = end
                yield None
                continue
        xstart = parser.xpath_offset()
        # print('START', xstart)
        # Hand it the actual token and ask for the ending offset.
        parser.feed(html[start:end])
        xend = parser.xpath_offset()
        # print('END', xend)
        prev_end = end

        # If we couldn't make progress then the xpaths generated are probably
        # incorrect. (If the parser doesn't make progress, then we can't rely
        # on the callbacks to have been called, which means we may not have
        # captured all state correctly.)
        #
        # Therefore, we simply give up and claim this token is not addressable.
        if not parser.made_progress:
            prev_progress = False
            yield None
        else:
            prev_progress = True
            yield XpathRange(xstart[0], xstart[1], xend[0], xend[1])
    parser.feed(html[prev_end:])
    parser.close()


def uni(s):
    if not isinstance(s, unicode):
        return unicode(s, 'utf-8')
    else:
        return s


def stream_item_roundtrip_xpaths(si, quick=False):
    '''Roundtrip all Xpath offsets in the given stream item.

    For every token that has both ``CHARS`` and ``XPATH_CHARS``
    offsets, slice the ``clean_html`` with the ``XPATH_CHARS`` offset
    and check that it matches slicing ``clean_visible`` with the
    ``CHARS`` offset.

    If this passes without triggering an assertion, then we're
    guaranteed that all ``XPATH_CHARS`` offsets in the stream item are
    correct. (Note that does not check for completeness. On occasion, a
    token's ``XPATH_CHARS`` offset cannot be computed.)

    There is copious debugging output to help make potential bugs
    easier to track down.

    This is used in tests in addition to the actual transform. It's
    expensive to run, but not running it means silent and hard to
    debug bugs.
    '''
    def debug(s):
        logger.warning(s)

    def print_window(token, size=200):
        coffset = token.offsets[OffsetType.CHARS]
        start = max(0, coffset.first - size)
        end = min(len(html), coffset.first + coffset.length + size)
        debug('-' * 49)
        debug(coffset)
        debug('window size: %d' % size)
        debug(html[start:end])
        debug('-' * 49)

    def debug_all(token, xprange, expected, err=None, got=None):
        debug('-' * 79)
        if err is not None:
            debug(err)
        debug(xprange)
        debug('expected: "%s"' % expected)
        if got is not None:
            debug('got: "%s"' % got)
        debug('token value: "%s"' % unicode(token.token, 'utf-8'))
        print_window(token, size=10)
        print_window(token, size=30)
        print_window(token, size=100)
        print_window(token, size=200)
        debug('-' * 79)

    def slice_clean_visible(token):
        coffset = token.offsets[OffsetType.CHARS]
        return cleanvis[coffset.first:coffset.first + coffset.length]

    def test_token(token):
        coffset = token.offsets.get(OffsetType.CHARS)
        if coffset is None:
            return False
        xoffset = token.offsets.get(OffsetType.XPATH_CHARS)
        if xoffset is None:
            return False
        crange = (coffset.first, coffset.first + coffset.length)
        xprange = XpathRange.from_offset(xoffset)
        expected = slice_clean_visible(token)
        if expected != unicode(token.token, 'utf-8'):
            # Yeah, apparently this can happen. Maybe it's a bug
            # in Basis? I'm trying to hustle, and this only happens
            # in two instances for the `random` document, so I'm not
            # going to try to reproduce a minimal counter-example.
            # ---AG
            return False
        try:
            got = xprange.slice_node(html_root)
        except InvalidXpathError as err:
            debug_all(token, xprange, expected, err=err)
            raise XpathMismatchError(html, cleanvis, xprange, crange)
        if expected != got:
            debug_all(token, xprange, expected, got=got)
            raise XpathMismatchError(html, cleanvis, xprange, crange)
        return True

    cleanvis = unicode(si.body.clean_visible, 'utf-8')
    html = unicode(si.body.clean_html, 'utf-8')
    html_root = XpathRange.html_node(html)
    total, has_valid_xpath = 0, 0
    for sentences in si.body.sentences.itervalues():
        for sentence in sentences:
            if quick:
                for i in xrange(len(sentence.tokens) - 1, -1, -1):
                    if test_token(sentence.tokens[i]):
                        break
            else:
                # Exhaustive test.
                for token in sentence.tokens:
                    total += 1
                    if test_token(token):
                        has_valid_xpath += 1
    if not quick:
        # This is nonsense if we have quick checking enabled.
        logger.info('stream item %s: %d/%d tokens with valid xpaths',
                    si.stream_id, has_valid_xpath, total)
