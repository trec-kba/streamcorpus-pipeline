
from streamcorpus import make_stream_item
from streamcorpus_pipeline._set_source import set_source

def test_set_source():

    stage = set_source(dict(
            new_source = 'super cow'
    ))

    si = make_stream_item(0, 'http://dogs.com/')
    si.source = 'foo'
    si2 = stage(si)
    assert si2.source == 'super cow'
