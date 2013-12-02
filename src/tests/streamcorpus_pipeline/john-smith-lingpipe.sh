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

## to make real tests, we need to dump a non-time-sensitive
## extract from these Chunk files and compare it to a stored
## copy of what is expected.
git checkout -- data/john-smith/john-smith-0.sc
git checkout -- data/john-smith/john-smith-tagged-by-lingpipe-0.sc
rm -f data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc
mkdir -p $TMP

echo data/john-smith/original | python -m streamcorpus_pipeline.run configs/john-smith-lingpipe.yaml
echo data/john-smith/john-smith-0.sc | python -m streamcorpus_pipeline.run configs/john-smith-lingpipe-from-chunk.yaml

## compare dumps
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-lingpipe-0.sc      --tokens > $TMP/lp-0.tsv
python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc --tokens > $TMP/lp-test-0.tsv

diff $TMP/lp-0.tsv $TMP/lp-test-0.tsv || die "failed: configs/john-smith-lingpipe.yaml"
diff $TMP/lp-0.tsv data/john-smith/john-smith-tagged-by-lingpipe-0.tsv || die "failed: configs/john-smith-lingpipe-from-chunk.yaml"
