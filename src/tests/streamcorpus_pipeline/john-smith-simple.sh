#!/bin/sh

set -e

cd $(dirname $0)/../../..

echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith.yaml
echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith-with-labels-from-tsv.yaml

## compare dumps
python -m streamcorpus.dump data/john-smith/john-smith-0.sc                      --field stream_id | sort > js-0.tsv
python -m streamcorpus.dump data/john-smith/john-smith-from-external-stage-0.sc  --field stream_id | sort > js-test-0.tsv

## verify that the ways of building JS create all the entries
diff js-0.tsv js-test-0.tsv && rm js-0.tsv js-test-0.tsv || { echo 'failed!'; exit 1; }

rm data/john-smith/john-smith-from-external-stage-0.sc

