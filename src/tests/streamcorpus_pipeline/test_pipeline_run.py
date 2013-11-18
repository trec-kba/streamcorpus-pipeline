import os
import pytest
import subprocess
from streamcorpus_pipeline._logging import logger

@pytest.mark.timeout(300)
@pytest.mark.parametrize(
    ('script_path', 'expect_success'), [
        ('john-smith-simple.sh', True), 
        ('john-smith-small-chunks.sh', True), 
        ('john-smith-broken-for-test.sh', False), 
        ('john-smith-taggers.sh', False)
    ], 
)
def test_pipeline_run(script_path, expect_success):
    cmd = os.path.join(os.path.dirname(__file__), script_path)
    logger.critical(cmd)
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    ret = None
    while 1:
        ret = p.poll()
        if ret is not None:
            break
        out, err = p.communicate()
        logger.critical( out )
        logger.critical( err )

    if expect_success:
        assert ret == 0
    else:
        assert ret != 0
