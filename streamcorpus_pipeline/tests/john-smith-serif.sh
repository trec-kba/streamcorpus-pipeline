#!/bin/sh

set -e

die () {
    echo >&2 "$@"
    exit 1
}
[ "$#" -eq 4 ] || die "1 argument required, $# provided"
TMP=$1

#serif_third=third/serif

#if [ ! -d "$serif_third" ]; then
#    echo "serif_third $serif_third is missing!"
#    exit 1
#fi

## to make real tests, we need to dump a non-time-sensitive
## extract from these Chunk files and compare it to a stored
## copy of what is expected.
#git checkout -- data/john-smith/john-smith-0.sc
#git checkout -- data/john-smith/john-smith-tagged-by-lingpipe-0.sc
#rm -f data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc
#mkdir -p $TMP

streamcorpus_pipeline -c $2/john-smith-serif-streamcorpus_one_step.yaml -i $3/john-smith/original --third $4 --tmp $1
streamcorpus_pipeline -c $2/john-smith-serif-streamcorpus_generate_serifxml.yaml -i $3/john-smith/john-smith-0.sc --third $4 --tmp $1
streamcorpus_pipeline -c $2/john-smith-serif-streamcorpus_read_serifxml.yaml -i $3/john-smith/john-smith-tagged-by-serifxml-only.sc --third $4 --tmp $1

## compare dumps
streamcorpus_dump $3/john-smith/john-smith-tagged-by-serif-0.sc      --tokens > $TMP/serif-0.tsv
streamcorpus_dump $3/john-smith/john-smith-tagged-by-serif-via-serifxml-0.sc    --tokens > $TMP/serif-test-0.tsv

diff $TMP/serif-0.tsv $3/john-smith/john-smith-tagged-by-serif-0.tsv	
diff $TMP/serif-0.tsv $TMP/serif-test-0.tsv || die "failed: configs/john-smith-serif-streamcorpus_read_serifxml.yaml"

## this is so slow that there must be something wrong with our
## build of Stanford CoreNLP
#python -m streamcorpus.pipeline.run -c configs/john-smith-stanford-from-chunk.yaml -i data/john-smith/john-smith-0.sc

