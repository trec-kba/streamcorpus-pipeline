import os
import pytest
import subprocess
from streamcorpus_pipeline._logging import logger

@pytest.mark.timeout(300)
@pytest.mark.parametrize(
    ('script_path',), [
        ('john-smith-simple.sh',), 
        #('john-smith-taggers.sh',)
    ], 
)
def test_pipeline_run(script_path):
    cmd = os.path.join(os.path.dirname(__file__), script_path)
    logger.critical(cmd)
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    ret = None
    while 1:
        ret = p.poll()
        if ret is not None:
            break
        out, err = p.communicate()
        print out
        print err
        
    assert ret == 0
