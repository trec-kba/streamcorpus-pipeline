

from streamcorpus import Chunk

def get_name_info(chunk_data, assert_one_date_hour=False):
    '''
    takes a chunk blob and obtains the date_hour, md5, num
    '''
    name_info = dict()

    ch = Chunk(data=chunk_data)
    date_hours = set()
    count = 0
    for si in ch:
        date_hours.add( si.stream_time.zulu_timestamp[:13] )
        count += 1

    ## create the md5 property, so we can use it in the filename
    name_info['md5'] = ch.md5_hexdigest
    name_info['num'] = count

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
