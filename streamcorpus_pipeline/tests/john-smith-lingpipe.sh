#!/bin/sh

set -e

die () {
    echo >&2 "$@"
    exit 1
}
[ "$#" -eq 4 ] || die "1 argument required, $# provided"
TMP=$1

## to make real tests, we need to dump a non-time-sensitive
## extract from these Chunk files and compare it to a stored
## copy of what is expected.
#git checkout -- data/john-smith/john-smith-0.sc
#git checkout -- data/john-smith/john-smith-tagged-by-lingpipe-0.sc
#rm -f data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc
#mkdir -p $TMP

streamcorpus_pipeline -c $2/john-smith-lingpipe.yaml -i $3/john-smith/original --third $4 --tmp $1  ## creates data/john-smith/john-smith-tagged-by-lingpipe-0-prebuild.sc
streamcorpus_pipeline -c $2/john-smith-lingpipe-from-chunk.yaml -i $3/john-smith/john-smith-0.sc  --third $4 --tmp $1

## compare dumps
streamcorpus_dump $3/john-smith/john-smith-tagged-by-lingpipe-0-prebuild.sc --tokens > $TMP/lp-0.tsv
streamcorpus_dump $3/john-smith/john-smith-tagged-by-lingpipe-test-0.sc --tokens > $TMP/lp-test-0.tsv

diff $TMP/lp-0.tsv $TMP/lp-test-0.tsv || die "failed: configs/john-smith-lingpipe.yaml"
diff $TMP/lp-0.tsv $3/john-smith/john-smith-tagged-by-lingpipe-0.tsv || die "failed: configs/john-smith-lingpipe-from-chunk.yaml"

## atomic rename to official location
#mv data/john-smith/john-smith-tagged-by-lingpipe-0-prebuild.sc data/john-smith/john-smith-tagged-by-lingpipe-0.sc
