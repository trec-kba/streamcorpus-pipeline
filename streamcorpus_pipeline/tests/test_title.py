
from streamcorpus import make_stream_item
from streamcorpus_pipeline._title import title
from streamcorpus_pipeline._clean_visible import clean_visible

def test_title():
    
    stage = title({})
    cv = clean_visible({})

    si = make_stream_item(0, '')
    si.body.clean_html = '''Then there
was a
<tag>   ...  <title>TITLE
HERE
  </title>
'''
    si = cv(si, {})
    si = stage(si)
    assert si.other_content['title'].clean_visible == 'TITLE HERE'
    
    si = make_stream_item(0, '')
    si.body.clean_html = '''Then there
was a
  that went <tag>   ...  <title>TITLE
HERE%s
  </title>
''' % ('*' * 80)
    si = cv(si, {})
    si = stage(si)
    assert si.other_content['title'].clean_visible == 'TITLE HERE' + '*' * 50 + '...'
    
