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


kaba.pipeline
-------------

The kba.pipeline python module contains tools for processing
streamcorpus.StreamItems stored in Chunks.  It includes transform
functions for getting clean_html, clean_visible text, creating labels
from hyperlinks to particular sites (e.g. Wikipedia), and taggers like
LingPipe and Stanford CoreNLP, that make Tokens and Sentences.


python3.3
---------
To create a python3 virtualenv, do this:

wget http://www.python.org/ftp/python/3.3.0/Python-3.3.0.tgz
untar and cd into dir
./configure --prefix /data/trec-kba/installs/py33
make install
wget http://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.8.4.tar.gz#md5=1c7e56a7f895b2e71558f96e365ee7a7
tar xzf virtualenv-1.8.4.tar.gz 
cd virtualenv-1.8.4/
/data/trec-kba/installs/py33/bin/python3 setup.py  install
cd ../../users/jrf/
/data/trec-kba/installs/py33/bin/virtualenv --distribute -p /data/trec-kba/installs/py33/bin/python3 py33

