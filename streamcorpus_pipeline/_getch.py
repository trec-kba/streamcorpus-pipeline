'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import sys, tty, termios

def getch():
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch
