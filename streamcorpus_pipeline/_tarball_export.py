'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import tarfile
import logging
import streamcorpus
from cStringIO import StringIO

from yakonfig import ConfigurationError

logger = logging.getLogger(__name__)

def tarball_export(config, t_path, name_info):
    t_path2 = t_path + '.tar.gz.tmp'
    tar = tarfile.open(name=t_path2, mode='w:gz')
    count = 0
    accepted_language_codes = config.get(
        'accepted_language_codes', [])
    for si in streamcorpus.Chunk(t_path):
        if accepted_language_codes and \
                si.body.language.code not in accepted_language_codes:
            logger.info('ignoring %s: %r', si.stream_id, si.body.language)
            continue

        if si.body.clean_html:
            export_text = si.body.clean_html
        elif si.body.clean_visible:
            export_text = si.body.clean_visible
        else:
            logger.info('ignoring %s: no clean_html nor clean_visible', si.stream_id)
            continue

        ## make stream_id available to tarinfo_name pattern
        name_info['stream_id'] = si.stream_id

        if export_text:
            ## create a file record
            data = StringIO(export_text)
            info = tar.tarinfo()
            ## make a name from the path and stream_id
            #'%s#%s' % (s3_path, si.stream_id)
            for k in ('name', 'uname', 'gname'):
                full= 'tarinfo_%s' % k
                if full not in config:
                    raise ConfigurationError(
                        'to_s3_tarballs requires the "%s" option.' % full)
            info.name  = config['tarinfo_name'] % name_info
            info.uname = config.get('tarinfo_uname')
            info.gname = config.get('tarinfo_gname')
            info.type = tarfile.REGTYPE
            info.mode = 0644
            info.mtime = si.stream_time.epoch_ticks
            info.size = len(export_text)
            tar.addfile(info, data)

            count += 1

    tar.close()
    logger.info('wrote %d texts to %s' % (count, t_path2))
    return t_path2
