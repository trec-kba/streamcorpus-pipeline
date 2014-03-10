'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''

import random

from streamcorpus import Chunk

def get_name_info(chunk_path, assert_one_date_hour=False, i_str=None):
    '''
    takes a chunk blob and obtains the date_hour, md5, num
    '''
    assert i_str is not None, 'must provide i_str as keyword arg'

    name_info = dict()

    i_fname = i_str.split('/')[-1]
    i_fname = i_fname.split('.')[0]  ## strip off .sc[.xz[.gpg]]
    name_info['input_fname'] = i_fname 

    name_info['input_md5'] = i_fname.split('-')[-1]

    # TODO: return a dict-like object that does the expensive
    # calculation lazily, the name format might not even need that
    # value.
    ch = Chunk(path=chunk_path, mode='rb')
    date_hours = set()
    target_names = set()
    doc_ids = set()
    epoch_ticks = None
    count = 0
    for si in ch:
        if epoch_ticks is None:
            epoch_ticks = si.stream_time.epoch_ticks
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
    name_info['epoch_ticks'] = epoch_ticks

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

    # TODO: in future lazy evaluation world, rand8 should return a
    # different value every time it is accessed so that a format could
    # be 'foo-{rand8}{rand8}'
    name_info['rand8'] = '%08x' % (random.randint(0, 0x7fffffff),)

    return name_info
