#!/bin/sh

set -e

cd $(dirname $0)/../..
export PYTHONPATH=$(dirname $0)/../..:$PYTHONPATH

python -m streamcorpus_pipeline.run -c configs/john-smith-small-chunks.yaml -i data/john-smith/original || { echo 'failed!'; exit 1; }
