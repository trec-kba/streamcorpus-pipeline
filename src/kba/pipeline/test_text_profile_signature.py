

from text_profile_signature import tps

def test_tps():
    path = os.path.dirname(__file__)
    path = os.path.join( path, '../../../data/test/' )
    text = open(os.path.join(path, 'nytimes-index-clean-visible.html')).read()
    print tps(text)
