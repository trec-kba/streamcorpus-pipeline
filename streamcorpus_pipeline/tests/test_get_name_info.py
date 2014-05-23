

from streamcorpus_pipeline._get_name_info import get_name_info
from streamcorpus import Chunk, make_stream_item

def test_get_name_info(tmpdir):

    path = str(tmpdir.join('test_path'))
    c = Chunk(path, mode='wb')
    c.add(make_stream_item(28491, 'abs_url'))

    name_info = get_name_info(path, i_str='foo')
    assert name_info['date_now'] == name_info['date_time_now'][:10]
    assert name_info['date_now'] + '-' + name_info['time_now'] == name_info['date_time_now']
