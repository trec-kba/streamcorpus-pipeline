
import os
import uuid
import pytest
from streamcorpus import Chunk, make_stream_item, ContentItem
#from _fpat_matcher import fpat_matcher # TODO: this fails before we can even fail the test
# TODO: probably delete this file


@pytest.mark.xfail  # pylint: disable=E1101
def test_matcher():
    
    config = dict(
        ## command to run
        fpat_path = 'cat'
        )

    fm = fpat_matcher( config )

    si1 = make_stream_item(None, 'http://example.com')
    si1.body = ContentItem( clean_visible = 'hello! This is a test of matching Bob.' )

    si2 = make_stream_item(None, 'http://example.com')
    si2.body = ContentItem( clean_visible = 'hello! This is a test of matching Sally.' )

    chunk_path = '/tmp/%s' % uuid.uuid1()
    
    ch = Chunk( chunk_path, mode='wb' )
    ch.add( si1 )
    ch.add( si1 )
    ch.add( si2 )
    ch.close()

    fm( chunk_path )

    ch = Chunk( chunk_path, mode='rb' )
    
    SIs = list(ch)
    
    ## verify the si has expected things
    for si in SIs:
        len( si.body.labels ) == 1

    for i in range(2):
        print SIs[i].ratings
        #SIs[i].ratings[0].annoannotator.annotator_id = 'fpat_matcher_TIMESTAMP'
        #SIs[i].ratings[0].target_id = 'http://en.wikipedia.org/wiki/Bob'

    #SIs[2].ratings[0].annoannotator.annotator_id = 'fpat_matcher_TIMESTAMP'
    #SIs[2].ratings[0].target_id = 'http://en.wikipedia.org/wiki/Sally'



