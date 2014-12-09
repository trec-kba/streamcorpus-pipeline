'''Generates metadata for use in naming files created by writers,
e.g. to_local_chunks and to_s3_chunks

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''

import random
import datetime
import hashlib
import logging
from streamcorpus import Chunk
import re

logger = logging.getLogger(__name__)

is_hex_32 = re.compile('[a-fA-F0-9]{32}')

def get_name_info(chunk_path, assert_one_date_hour=False, i_str=None,
                  chunk_type=Chunk):
    '''
    takes a chunk blob and obtains the date_hour, md5, num

    makes fields:
    i_str
    input_fname
    input_md5 - parsed from input filename if it contains '-%(md5)s-'
    md5
    num
    epoch_ticks
    target_names
    doc_ids_8
    date_hour
    rand8
    date_now
    time_now
    date_time_now
    '''
    assert i_str is not None, 'must provide i_str as keyword arg'

    name_info = dict()
    if i_str:
        name_info['i_str'] = i_str
    else:
        name_info['i_str'] = ''

    i_fname = i_str.split('/')[-1]
    i_fname = i_fname.split('.')[0]  ## strip off .sc[.xz[.gpg]]
    name_info['input_fname'] = i_fname 

    input_md5s = []
    for part in i_fname.split('-'):
        if len(part) == 32 and is_hex_32.match(part):
            input_md5s.append(part)
    name_info['input_md5'] = '-'.join(input_md5s)

    # TODO: return a dict-like object that does the expensive
    # calculation lazily, the name format might not even need that
    # value.
    ch = chunk_type(path=chunk_path, mode='rb')
    date_hours = set()
    target_names = set()
    doc_ids = set()
    epoch_ticks = None
    count = 0
    try:
        for si in ch:
            if chunk_type is Chunk:
                if epoch_ticks is None:
                    epoch_ticks = si.stream_time.epoch_ticks
                date_hours.add( si.stream_time.zulu_timestamp[:13] )
                doc_ids.add( si.doc_id )
                for annotator_id, ratings in si.ratings.items():
                    for rating in ratings:
                        target_name = rating.target.target_id.split('/')[-1]
                        target_names.add( target_name )
            count += 1
    except Exception, exc:
        logger.critical('failed to iter over chunk', exc_info=True)
        

    ## create the md5 property, so we can use it in the filename
    if hasattr(ch, 'md5_hexdigest'):
        name_info['md5'] = ch.md5_hexdigest
    else:
        try:
            data = open(chunk_path).read()
            name_info['md5'] = hashlib.md5(data).hexdigest()
        except Exception, exc:
            logger.critical('failed to compute md5', exc_info=True)
            name_info['md5'] = 'broken'
    name_info['num'] = count
    name_info['epoch_ticks'] = epoch_ticks

    name_info['target_names'] = '-'.join( target_names )
    name_info['doc_ids_8'] = '-'.join( [di[:8] for di in doc_ids] )

    if chunk_type is Chunk:
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
    else:
        name_info['date_hour'] = 'NO-DATE-HOUR-FOR-FC'

    # TODO: in future lazy evaluation world, rand8 should return a
    # different value every time it is accessed so that a format could
    # be 'foo-{rand8}{rand8}'
    name_info['rand8'] = '%08x' % (random.randint(0, 0x7fffffff),)

    name_info['date_now'] = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    name_info['time_now'] = datetime.datetime.utcnow().strftime('%H-%M-%S')
    name_info['date_time_now'] = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')

    return name_info
