import os
import time
import pytest
import subprocess
from streamcorpus_pipeline._logging import logger

@pytest.mark.parametrize(
    ('script_path', 'expect_success'), [
        ('john-smith-simple.sh', True), 
        ('john-smith-small-chunks.sh', True), 
        ('john-smith-broken-for-test.sh', False), 
        ('john-smith-taggers.sh', True)
    ], 
)
def test_pipeline_run(script_path, expect_success):
    cmd = os.path.join(os.path.dirname(__file__), script_path)
    logger.critical(cmd)
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    ret = None
    start_time = time.time()
    while time.time() - start_time < 300:
        ret = p.poll()
        if ret is not None:
            break
        out, err = p.communicate()
        logger.critical( out )
        logger.critical( err )

    if time.time() - start_time >= 900:
        raise Exception('timed out after %d seconds' % (time.time() - start_time))

    if expect_success:
        assert ret == 0
    else:
        assert ret != 0
