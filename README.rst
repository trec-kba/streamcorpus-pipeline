TREC KBA Data
=============

kba.pipeline is a document processing pipeline that assembles
streamcorpus objects from raw data sets for us in TREC KBA.




Wikipedia Links Corpus (WLC)
----------------------------

First step: To make use of the WLC data released by Google, we
transform the original inputs into 10888934 streamcorpus objects, and
grab the raw from the 10833242 thrift messages provided by UMass Brian
Martin's initial fetch.  This is implemented in
trec-kba-data/import_wlc.py, which is run in Condor by
run_import_wlc.{sh,submit}




