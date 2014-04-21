#!/bin/sh

set -e

streamcorpus_pipeline -c $2/john-smith-broken-for-test.yaml -i $3/john-smith/original || { echo 'failed!'; exit 1; }
