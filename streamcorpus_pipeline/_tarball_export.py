'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import tarfile
import logging
import streamcorpus
from cStringIO import StringIO

logger = logging.getLogger(__name__)

def tarball_export(t_path, name_info):
    t_path2 = t_path + '.tar.gz.tmp'
    tar = tarfile.open(name=t_path2, mode='w:gz')
    count = 0
    for si in streamcorpus.Chunk(t_path):
        if si.body.clean_html:
            export_text = si.body.clean_html
        elif si.body.clean_visible:
            export_text = si.body.clean_visible
        else:
            export_text = None

        if export_text:
            ## create a file record
            data = StringIO(export_text)
            info = tar.tarinfo()
            ## make a name from the path and stream_id
            info.name = '%s#%s' % (name_info['s3_output_path'], si.stream_id)
            info.uname = 'jrf'
            info.gname = 'trec-kba'
            info.type = tarfile.REGTYPE
            info.mode = 0644
            info.mtime = si.stream_time.epoch_ticks
            info.size = len(export_text)
            tar.addfile(info, data)

            count += 1

    tar.close()
    logger.info('wrote %d texts to %s' % (count, t_path2))
    return t_path2
