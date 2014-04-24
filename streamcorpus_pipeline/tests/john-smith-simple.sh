#!/bin/sh

set -e

echo $3/john-smith/original | streamcorpus_pipeline -c $2/john-smith.yaml -i -
streamcorpus_pipeline -c $2/john-smith-with-labels-from-tsv.yaml -i $3/john-smith/original

## compare dumps
streamcorpus_dump $3/john-smith/john-smith-0.sc                      --field stream_id | sort > js-0.tsv
streamcorpus_dump $3/john-smith/john-smith-from-external-stage-0.sc  --field stream_id | sort > js-test-0.tsv

## verify that the ways of building JS create all the entries
diff js-0.tsv js-test-0.tsv && rm js-0.tsv js-test-0.tsv || { echo 'failed!'; exit 1; }

#rm data/john-smith/john-smith-from-external-stage-0.sc

