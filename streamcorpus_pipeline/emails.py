'''Protect emails <user@email.com> from tag strippers.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
import re
from uuid import uuid4

bracket_emails = re.compile(
    '''<[\w-]+(?:\.[\w-]+)*@(?:[\w-]+\.)+[a-zA-Z0-9]{2,7}>''',
    re.I)

def fix_emails(text):
    '''Replace all angle bracket emails with a unique key.'''
    emails = bracket_emails.findall(text)

    keys = []
    for email in emails:
	_email = email.replace("<","&lt;").replace(">","&gt;")
        text = text.replace(email, _email)

    return text
