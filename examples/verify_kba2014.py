'''rejester task for iterating over the KBA 2014 corpus and saving
stats files

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''

from __future__ import division
import gzip
import json
import logging
import os
import re
from streamcorpus import Chunk
from subprocess import Popen, PIPE
import sys
import time
import traceback
import yaml
import uuid

logging.basicConfig()

work_spec = {
    'name': 'verify_kba2014',
    'desc': 'Verify chunks from the KBA 2014 streamcorpus.',
    'min_gb': 1,
    'config': None,
    'module': 'verify_kba2014',
    'run_function': 'rejester_run',
    'terminate_function': 'rejester_terminate',
}

def rejester_run(work_unit):
     '''get a rejester.WorkUnit with KBA s3 path, fetch it, and save
     some counts about it.
     '''
     #fname = 'verify-chunks-%d-%d' % (os.getpid(), time.time())
     fname = work_unit.key.strip().split('/')[-1]
     
     output_dir_path = work_unit.data.get('output_dir_path', '/mnt')
     u = uuid.uuid3(uuid.UUID(int=0), work_unit.key.strip())
     path1 = u.hex[0]
     path2 = u.hex[1]
     fpath = os.path.join(output_dir_path, path1, path2, fname)
     if not os.path.exists(os.path.dirname(fpath)):
          os.makedirs(os.path.dirname(fpath))

     output = gzip.open(fpath + '-out.gz', 'wb')

     expected_si_count = int(fname.split('-')[1])

     max_tries = 20
     tries = 0
     while tries < max_tries:
          try:
               exc, si_count, serif_count, clean_visible_bytes, clean_visible_count, stream_ids = \
                   attempt_fetch(work_unit, fpath)
               if si_count != expected_si_count:
                    print 'retrying because si_count = %d != %d expected_si_count' % (si_count, expected_si_count)
                    sys.stdout.flush()
                    tries += 1
                    continue
               else:
                    print 'succeeded in reading si_count = %d' % (si_count,)
                    sys.stdout.flush()
               output.write( '%s\t%d\t%d\t%d\t%d\t%s\t%s\n' % (
                         exc, si_count, serif_count, clean_visible_bytes, clean_visible_count, 
                         work_unit.key.strip(), ','.join(['%s|%s' % tup for tup in stream_ids])) )
               break
          except Exception, exc:
               print 'broken?'
               print traceback.format_exc(exc)
               sys.stdout.flush()
               tries += 1
               output.write(traceback.format_exc(exc))

     output.close()

def attempt_fetch(work_unit, fpath):
     '''attempt a fetch and iteration over a work_unit.key path in s3
     '''
     url = 'http://s3.amazonaws.com/aws-publicdatasets/' + work_unit.key.strip()

     ## cheapest way to iterate over the corpus is a few stages of
     ## streamed child processes.  Note that stderr needs to go
     ## separately to a file so that reading the stdin doesn't get
     ## blocked:
     cmd = '(wget -O - %s | gpg --no-permission-warning --trust-model always --output - --decrypt - | xz --decompress) 2> %s-err' % (url, fpath)
     print cmd
     child = Popen(cmd, stdout=PIPE, shell=True)
     print 'child launched'
     sys.stdout.flush()

     si_count = 0
     serif_count = 0
     exc = ''
     stream_ids = list()
     clean_visible_bytes = 0
     clean_visible_count = 0
     try:
         for si in Chunk(file_obj=child.stdout):
             print si.stream_id, si.abs_url
             if si.body.language:
                 lang = si.body.language.code
             else:
                 lang = ''
             stream_ids.append((lang, si.stream_id))
             if si.body.clean_visible:
                 clean_visible_count += 1
                 clean_visible_bytes += len(si.body.clean_visible)
             si_count += 1
             if 'serif' in si.body.sentences:
                 serif_count += 1
     except Exception, exc:
         exc = re.sub('\s+', ' ', str(exc)).strip()

     child.terminate()
     child.wait()
     child.stdout.close()
     return exc, si_count, serif_count, clean_visible_bytes, clean_visible_count, stream_ids


def rejester_terminate(work_unit):
     'required for rejester, but no-op here'
     pass



if __name__ == '__main__':
     import argparse
     parser = argparse.ArgumentParser()
     parser.add_argument('--run', action='store_true', default=False, help='take S3 paths over stdin')
     args = parser.parse_args()

     if args.run:
          print 'generating simulated WorkUnits for processing the corpus'
          class Empty():
               pass
          for line in sys.stdin:
               print 'running', line
               sys.stdout.flush()
               work_unit = Empty()
               work_unit.data = dict(output_dir_path = '/mnt')
               work_unit.key = line.strip()
               rejester_run(work_unit)
          
     else:
          ws_path = 'verify_kba2014_work_spec.yaml'
          yaml.dump(work_spec, open(ws_path, 'wb'))
          print 'dumped a WorkSpec for use with rejester to %s' % ws_path

          wus_path = 'verify_kba2014_work_units.json'
          wus = open(wus_path, 'wb')
          count = 0
          for line in sys.stdin:
               count += 1
               wu = {line.strip(): dict(output_dir_path='/mnt')}
               wus.write(json.dumps(wu) + '\n')
          wus.close()

          print 'created %d records in %s' % (count, wus_path)
