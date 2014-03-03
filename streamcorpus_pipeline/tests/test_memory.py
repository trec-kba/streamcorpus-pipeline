
from streamcorpus_pipeline import _memory

def test_memory():
    print 'VmSize: %d bytes' % _memory.memory()
    print 'VmRSS:  %d bytes' % _memory.resident()
    print 'VmStk:  %d bytes' % _memory.stacksize()
    
