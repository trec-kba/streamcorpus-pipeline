'''
streamcorpus_pipline self-tests

Your use of this software is governed by your license agreement.

Copyright 2012-2014 Diffeo, Inc.
'''
import argparse
import os
import pytest
import sys

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('redis_address',
                        help='hostname:port of a redis instance to use as '
                             'kvlayer backend and registry backend. Running '
                             'the tests requires a redis instance accessible '
                             'on the network.')
    parser.add_argument('elastic_address', help='hostname:port of ES')
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
        '--redis-address', args.redis_address,
        '--elastic-address', args.elastic_address,
    ]

    if args.third_dir:
        cmd.extend(['--third-dir', args.third_dir, '--run-integration'])
    cmd.append(test_dir)
    response = pytest.main(cmd)
    if response:
        sys.exit(response)


if __name__ == '__main__':
    main()
