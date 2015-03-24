'''Protect emails <user@email.com> from tag strippers.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
import re
from uuid import uuid4

bracket_emails = re.compile(
    '''<[\w-]+(?:\.[\w-]+)*@(?:[\w-]+\.)+[a-zA-Z0-9]{2,7}>''',
    re.I)

def key_emails(text):
    '''Replace all angle bracket emails with a unique key.'''
    emails = bracket_emails.findall(text)

    keys = []
    for email in emails:
        key = str(uuid4())
        keys.append(key)
        text = text.replace(email, '#EMAIL-'+key)

    return text, keys, emails

def replace_keys(text, keys, emails):
    '''Replace email keys with original emails'''
    numkeys = len(keys)
    for xin in range(0, numkeys):
        text = text.replace('#EMAIL-'+keys[xin], emails[xin])

    return text

