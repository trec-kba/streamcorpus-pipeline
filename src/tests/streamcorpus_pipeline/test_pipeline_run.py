import os
import pytest
import subprocess

@pytest.mark.timeout(60)
def test_pipeline_run():
    cmd = os.path.join(os.path.dirname(__file__), 'john-smith-simple.sh')
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
    
