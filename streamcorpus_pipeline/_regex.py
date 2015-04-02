'''Protect emails <user@email.com> from tag strippers.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
import re
from uuid import uuid4

#    r"""<img[^>]+src\s*=\s*['"]([^'"]+)['"][^>]*>""",
BRACKET_EMAILS = re.compile(
    '''<[\w-]+(?:\.[\w-]+)*@(?:[\w-]+\.)+[a-zA-Z0-9]{2,7}>''',
    re.I)

HTML_IMAGES = re.compile(
    r"""<img[^>]+\s*src\s*=[^>]*>""",
    re.I)

NEWLINES = re.compile('''^\n\n$''')
SPACES = re.compile('''\s\s\s\s''')

def tidy_text(text):
    t=NEWLINES.sub('\n',text)
    t=SPACES.sub(' ',t)
    return t

def re_key_tokens(text, prefix="#TOKEN-", pattern=BRACKET_EMAILS):
    '''Replace all pattern tokens with a unique key.'''
    tokens = pattern.findall(text)
    print "TOKENS: "+str(tokens)
    keys = []
    for token in tokens:
        key = str(uuid4())
        keys.append(key)
        text = text.replace(token, prefix+key)

    return text, keys, tokens

def re_replace_keys(text, keys, tokens, prefix="#TOKEN-",prepad="",postpad=""):
    '''Replace token keys with original token'''
    numkeys = len(keys)
    for xin in range(0, numkeys):
        text = text.replace(prefix+keys[xin], prepad+tokens[xin]+postpad)

    return text

