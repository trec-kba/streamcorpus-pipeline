#!/bin/sh

set -e

cd $(dirname $0)/../..
export PYTHONPATH=$(dirname $0)/../..:$PYTHONPATH

(echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith-small-chunks.yaml) || { echo 'failed!'; exit 1; }
