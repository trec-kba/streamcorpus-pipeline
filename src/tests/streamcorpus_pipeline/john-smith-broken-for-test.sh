#!/bin/sh

set -e

cd $(dirname $0)/../../..
export PYTHONPATH=$(dirname $0)/../..:$PYTHONPATH

(echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith-broken-for-test.yaml) || { echo 'failed!'; exit 1; }
