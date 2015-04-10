from __future__ import absolute_import, division, print_function

from HTMLParser import HTMLParser
from itertools import imap

from streamcorpus import XpathRange


# To be written transform.
class xpath_offsets(object):
    config_name = 'xpath_offsets'
    default_config = {}

    def __init__(self, config):
        self.config = config

    def __call__(self, fc):
        pass


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
        # depth |--> [tag | data], where `data` is represented via `None`
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
        self.depth_stack = {0: []}

    # This is the one public method!
    def xpath_offset(self):
        '''Returns a tuple of ``(xpath, character offset)``.

        The ``xpath`` returned *uniquely* identifies the end of the
        text node most recently inserted. The character offsets
        indicates where the text inside the node ends. (When the text
        node is empty, the offset returned is `0`.)
        '''
        datai = self.text_index()
        return (uni(self.xpath() + '/text()[%d]' % datai), self.data_start)

    # These help with xpath generation.
    def xpath(self):
        return '/' + '/'.join(imap(lambda (d, tag): self.xpath_node(d, tag),
                                   enumerate(self.xpath_stack())))

    def xpath_stack(self):
        for i in xrange(self.depth):
            yield self.depth_stack[i][-1]

    def text_index(self):
        i = 1
        last_is_data = False
        for v in self.depth_stack[self.depth]:
            if v is None and not last_is_data:
                last_is_data = True
            elif v is not None and last_is_data:
                i += 1
                last_is_data = False
        return i

    def xpath_node(self, depth, tag):
        return '%s[%d]' % (tag, self.tag_count(depth, tag))

    def tag_count(self, depth, tag):
        return self.depth_stack[depth].count(tag)

    # Satisfy the `HTMLParser` interface.
    def handle_starttag(self, tag, attrs):
        self.depth_stack[self.depth].append(tag)
        self.data_start = 0

        self.depth += 1
        self.depth_stack[self.depth] = []

    def handle_endtag(self, tag):
        self.depth_stack.pop(self.depth)
        self.data_start = 0
        self.depth -= 1

    def handle_data(self, data):
        assert isinstance(data, unicode)  # important for ensuring char offsets
        self.depth_stack[self.depth].append(None)
        self.data_start += len(data)


def tokens_to_xpaths(html, char_offsets):
    '''Converts HTML and a sequence of char offsets to xpath offsets.

    Returns a generator of :class:`streamcorpus.XpathRange` objects
    in correspondences with the sequence of ``char_offsets`` given.
    Namely, each ``XpathRange`` should address precisely the same
    text as that ``char_offsets`` (sans the HTML).

    ``char_offsets`` must be a sorted and non-overlapping sequence
    of character ranges. They do not have to be contiguous.
    '''
    html = uni(html)
    parser = XpathTextCollector()
    prev_end = 0
    for start, end in char_offsets:
        parser.feed(html[prev_end:start])
        xstart = parser.xpath_offset()
        parser.feed(html[start:end])
        prev_end = end
        xend = parser.xpath_offset()
        yield XpathRange(xstart[0], xstart[1], xend[0], xend[1])
    parser.feed(html[prev_end:])
    parser.close()


def uni(s):
    if not isinstance(s, unicode):
        return unicode(s, 'utf-8')
    else:
        return s
