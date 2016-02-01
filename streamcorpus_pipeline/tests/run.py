'''
streamcorpus_pipline self-tests

Your use of this software is governed by your license agreement.

Copyright 2012-2016 Diffeo, Inc.
'''
import argparse
import os
import pytest
import sys

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--third-dir', help='path to third-party tools directory', default=None)
    args = parser.parse_args()

    test_dir = os.path.dirname(__file__)
    try:
        import pytest_incremental
    except:
        pytest_incremental = None

    cmd = ['-vv']
    if pytest_incremental is not None:
        cmd += ['-n', '8']
    cmd += [
        '--runslow', '--runperf',
    ]

    if args.third_dir:
        cmd.extend(['--third-dir', args.third_dir, '--run-integration'])
    cmd.append(test_dir)
    response = pytest.main(cmd)
    if response:
        sys.exit(response)


if __name__ == '__main__':
    main()
