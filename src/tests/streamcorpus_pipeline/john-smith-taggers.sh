#!/bin/sh

set -e

cd $(dirname $0)/../../..

## to make real tests, we need to dump a non-time-sensitive
## extract from these Chunk files and compare it to a stored
## copy of what is expected.
git checkout -- data/john-smith/john-smith-0.sc
git checkout -- data/john-smith/john-smith-tagged-by-lingpipe-0.sc
rm -f data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc
rm -rf tmp
mkdir tmp

echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith-lingpipe.yaml
echo data/john-smith/john-smith-0.sc | python -m streamcorpus_pipeline.run configs/john-smith-lingpipe-from-chunk.yaml

## compare dumps
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-lingpipe-0.sc      --tokens > tmp/lp-0.tsv
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc --tokens > tmp/lp-test-0.tsv

diff tmp/lp-0.tsv tmp/lp-test-0.tsv || { echo 'failed!'; exit 1; }
diff tmp/lp-0.tsv data/john-smith/john-smith-tagged-by-lingpipe-0.tsv || { echo 'failed!'; exit 1; }

echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith-serif-streamcorpus_one_step.yaml
echo data/john-smith/john-smith-0.sc | python -m streamcorpus_pipeline.run configs/john-smith-serif-streamcorpus_generate_serifxml.yaml
echo data/john-smith/john-smith-tagged-by-serifxml-only.sc | python -m streamcorpus_pipeline.run configs/john-smith-serif-streamcorpus_read_serifxml.yaml

## compare dumps
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-serif-0.sc      --tokens > tmp/serif-0.tsv
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-serif-via-serifxml-0.sc    --tokens > tmp/serif-test-0.tsv

diff tmp/serif-0.tsv data/john-smith/john-smith-tagged-by-serif-0.tsv	
diff tmp/serif-0.tsv tmp/serif-test-0.tsv || { echo 'failed!'; exit 1; }

## this is so slow that there must be something wrong with our
## build of Stanford CoreNLP
#echo data/john-smith/john-smith-0.sc | python -m streamcorpus.pipeline.run configs/john-smith-stanford-from-chunk.yaml

rm -rf tmp
