

from streamcorpus import Chunk

def get_name_info(chunk_path, assert_one_date_hour=False):
    '''
    takes a chunk blob and obtains the date_hour, md5, num
    '''
    name_info = dict()

    ch = Chunk(path=chunk_path, mode='rb')
    date_hours = set()
    target_names = set()
    doc_ids = set()
    count = 0
    for si in ch:
        date_hours.add( si.stream_time.zulu_timestamp[:13] )
        doc_ids.add( si.doc_id )
        for annotator_id, ratings in si.ratings.items():
            for rating in ratings:
                target_name = rating.target.target_id.split('/')[-1]
                target_names.add( target_name )
        count += 1

    ## create the md5 property, so we can use it in the filename
    name_info['md5'] = ch.md5_hexdigest
    name_info['num'] = count

    name_info['target_names'] = '-'.join( target_names )
    name_info['doc_ids_8'] = '-'.join( [di[:8] for di in doc_ids] )

    if assert_one_date_hour:
        assert len(date_hours) == 1, \
            'got a chunk with other than one data_hour! ' + \
            repr(date_hours)

    if len(date_hours) > 0:
        date_hour = list(date_hours)[0]
        date_hour = date_hour.replace('T', '-')
    else:
        assert count == 0, (date_hours, count)
        date_hour = None
    name_info['date_hour'] = date_hour

    return name_info
