#!/usr/bin/env python
'''
Converts HTML-escaped and XML-escaped characters into their simple
character equivalents.  Borrows heavily from the BeautifulSoup
library, which has an MIT/X11 License.

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import re
import sys
import cgi
from htmlentitydefs import name2codepoint

XML_ENTITIES_TO_SPECIAL_CHARS = { "apos" : "'",
                                  "quot" : '"',
                                  "amp" : "&",
                                  "lt" : "<",
                                  "gt" : ">" }

tags = re.compile(r"&(#\d+|#x[0-9a-fA-F]+|\w+);")

ENTITIES_THAT_ARE_SAFE_TO_STRING_PAD = [
    "nbsp", "amp", "quot", "gt", "raquo", "lt", "ldquo", "rdquo",
    "bull", "hellip", "laquo", "middot", "rsquo", "ndash", "mdash",
    "curren", "bdquo", "lsquo", "copy", "rarr", "apos", "reg"]


def html_entities_to_unicode(text, space_padding=False, safe_only=False):
    '''
    Convert any HTML, XML, or numeric entities in the attribute values.
    For example '&amp;' becomes '&'.

    This is adapted from BeautifulSoup, which should be able to do the
    same thing when called like this --- but this fails to convert
    everything for some bug.

    text = unicode(BeautifulStoneSoup(text, convertEntities=BeautifulStoneSoup.XML_ENTITIES))
    '''
    def convert_entities(match):
        '''
        comes from BeautifulSoup.Tag._convertEntities
        '''
        x = match.group(1)

        if safe_only and x not in ENTITIES_THAT_ARE_SAFE_TO_STRING_PAD:
            return u'&%s;' % x

        if x in name2codepoint:
            ## handles most cases
            return unichr(name2codepoint[x])

        elif x in XML_ENTITIES_TO_SPECIAL_CHARS:
            return XML_ENTITIES_TO_SPECIAL_CHARS[x]

        elif len(x) > 0 and x[0] == '#':
            # Handle numeric entities
            if len(x) > 1 and x[1] == 'x':
                return unichr(int(x[2:], 16))
            else:
                return unichr(int(x[1:]))
        else:
            ## uh oh, failed to anything
            return u'&%s;' % x

    def convert_to_padded_entitites(match):
        converted_string = convert_entities(match)
        num_spaces_needed = len(match.group(0)) - len(converted_string)
        assert num_spaces_needed >= 0, \
            'len(%r) !<= len(%r)' % (converted_string, match.group(0))
        ## Where to put the spaces?  Before, after, symmetric?
        # Let's do symmetric.
        ## cast to int in prep for python3
        num_left = int(num_spaces_needed / 2)
        num_right = num_spaces_needed - num_left
        return (' ' * num_left) + converted_string + (' ' * num_right)

    ## brute force regex through all the characters...
    if space_padding:
        return tags.sub(
                      convert_to_padded_entitites,
                      text)
    else:
        return tags.sub(
                      convert_entities,
                      text)

'''
def html_entities_to_unicode(text, safe_wrap=False, space_padding=False):
    if not safe_wrap:
        return _html_entities_to_unicode(text, space_padding=space_padding)

    else:
        ## iterate byte by byte looking for possible entities
        new_text = list()
        c = 0
        p = 0
        buf = bytes()
        while c < len(text):
            if text[c] == '&':
                ## capture all bytes up to this point
                if buf:
                    ## inside of buffer that cannot be a valid entity;
                    ## accumulate buffer, there will be no replacement
                    new_text.append(buf)
                    buf = r''
                else:    # start of possible entity
                    ## get text up to this point
                    new_text.append(text[p:c])
                    ## move previous cursor forward
                    p = c
                    buf = r'&'

            elif text[c] == ';': # finished a possible entity
                buf += ';'
                try:
                    #print buf
                    replacement = _html_entities_to_unicode(
                        buf, space_padding=space_padding)
                except Exception, exc:
                    #print exc
                    #logger.warning('html_entities_to_unicode: %r --> %r' % (buf, exc))
                    ## do not replace, just accumulate what was in the buffer
                    new_text.append(buf)
                else:
                    ## accumulate the replacement
                    new_text.append(replacement)
                    ## move previous cursor forward plus one for the ';'
                    p = c + 1
                ## reset state machine
                buf = r''

            elif buf:    # inside of a possible entity
                buf += text[c]
            # not inside an entity, so continue accumulating new text
            ## continue incrementing through the input text
            c += 1
        ## all done!  return the new text
        new_text.append(buf)
        buf = bytearray()
        for portion in new_text:
            buf += portion.encode('utf8')
        return buf
'''

def unicode_to_html_entities(text):
    '''Converts unicode to HTML entities.  For example '&' becomes '&amp;'.'''
    text = cgi.escape(text).encode('ascii', 'xmlcharrefreplace')
    return text



if __name__ == '__main__':
    '''
    Run basic tests
    '''
    text = '&amp;, &reg;, &lt;, &gt;, &cent;, &pound;, &yen;, &euro;, &sect;, &copy;'

    uni = html_entities_to_unicode(text)
    htmlent = unicode_to_html_entities(uni)

    print uni
    print htmlent
    #print html_entities_to_unicode('&#8364;', safe_wrap=True)

    ## verify inverted
    uni2 = html_entities_to_unicode(htmlent)
    assert uni == uni2, uni2

    print html_entities_to_unicode(htmlent, space_padding=True)
    ## but does not invert
    print unicode_to_html_entities(
        html_entities_to_unicode(htmlent, space_padding=True))


    # &amp;, &#174;, &lt;, &gt;, &#162;, &#163;, &#165;, &#8364;, &#167;, &#169;


