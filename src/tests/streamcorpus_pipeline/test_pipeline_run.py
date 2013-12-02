import os
import time
import uuid
import shutil
import pytest
import subprocess
from streamcorpus_pipeline._logging import logger

@pytest.fixture(
    scope = 'function',
    params = [
        ('john-smith-simple.sh', True), 
        ('john-smith-small-chunks.sh', True), 
        ('john-smith-broken-for-test.sh', False), 
        ('john-smith-lingpipe.sh', True),
        ('john-smith-serif.sh', True),
    ],
)
def cmd_expect_success(request):
    script_name, expect_success = request.param
    cmd = os.path.join(os.path.dirname(__file__), script_name)
    tmp_dir = '/tmp/' + uuid.uuid4().hex
    cmd += ' ' + tmp_dir
    def fin():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    request.addfinalizer(fin)
    return (cmd, expect_success)

def test_pipeline_run(cmd_expect_success):
    cmd, expect_success = cmd_expect_success    
    logger.critical(cmd)
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    ret = None
    start_time = time.time()
    max_time = 900
    elapsed = 0
    while elapsed < max_time:
        elapsed = time.time() - start_time
        ret = p.poll()
        if ret is not None:
            break
        out, err = p.communicate()
        logger.critical( out )
        logger.critical( err )

    if elapsed >= max_time:
        raise Exception('timed out after %d seconds' % (time.time() - start_time))

    if expect_success:
        assert ret == 0
    else:
        assert ret != 0
