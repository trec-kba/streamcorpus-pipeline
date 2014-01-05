#!/bin/sh

set -e

die () {
    echo >&2 "$@"
    exit 1
}
[ "$#" -eq 1 ] || die "1 argument required, $# provided"
TMP=$1

cd $(dirname $0)/../../..

export PYTHONPATH=$(dirname $0)/../..:$PYTHONPATH

#serif_third=third/serif

#if [ ! -d "$serif_third" ]; then
#    echo "serif_third $serif_third is missing!"
#    exit 1
#fi

## to make real tests, we need to dump a non-time-sensitive
## extract from these Chunk files and compare it to a stored
## copy of what is expected.
git checkout -- data/john-smith/john-smith-0.sc
git checkout -- data/john-smith/john-smith-tagged-by-lingpipe-0.sc
rm -f data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc
mkdir -p $TMP

echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith-serif-streamcorpus_one_step.yaml
echo data/john-smith/john-smith-0.sc | python -m streamcorpus_pipeline.run configs/john-smith-serif-streamcorpus_generate_serifxml.yaml
echo data/john-smith/john-smith-tagged-by-serifxml-only.sc | python -m streamcorpus_pipeline.run configs/john-smith-serif-streamcorpus_read_serifxml.yaml

## compare dumps
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-serif-0.sc      --tokens > $TMP/serif-0.tsv
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-serif-via-serifxml-0.sc    --tokens > $TMP/serif-test-0.tsv

diff $TMP/serif-0.tsv data/john-smith/john-smith-tagged-by-serif-0.tsv	
diff $TMP/serif-0.tsv $TMP/serif-test-0.tsv || die "failed: configs/john-smith-serif-streamcorpus_read_serifxml.yaml"

## this is so slow that there must be something wrong with our
## build of Stanford CoreNLP
#echo data/john-smith/john-smith-0.sc | python -m streamcorpus.pipeline.run configs/john-smith-stanford-from-chunk.yaml

