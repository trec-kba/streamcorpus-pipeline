TREC KBA Data
=============

kba.pipeline is a document processing pipeline that assembles
streamcorpus objects from raw data sets for us in TREC KBA.


kba.pipeline
-------------

The kba.pipeline python module contains tools for processing
streamcorpus.StreamItems stored in Chunks.  It includes transform
functions for getting clean_html, clean_visible text, creating labels
from hyperlinks to particular sites (e.g. Wikipedia), and taggers like
LingPipe and Stanford CoreNLP, that make Tokens and Sentences.

python2.7
---------
To create a python2.7 virtualenv, do this:

wget http://www.python.org/ftp/python/2.7.3/Python-2.7.3.tgz
tar xzf Python-2.7.3.tgz
cd Python-2.7
./configure --prefix /data/trec-kba/installs/py27
make install
cd ..
wget http://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.8.4.tar.gz#md5=1c7e56a7f895b2e71558f96e365ee7a7
tar xzf virtualenv-1.8.4.tar.gz 
cd virtualenv-1.8.4/
/data/trec-kba/installs/py27/bin/python setup.py  install
cd ..
/data/trec-kba/installs/py27/bin/virtualenv --distribute -p /data/trec-kba/installs/py27/bin/python py27env


Easiest to put this entire repo at a path like

    /data/trec-kba/installs/trec-kba-data

which is hardcoded into these three files:

    scripts/spinn3r-transform.sh
    scripts/spinn3r-transform.submit
    configs/spinn3r-transform.yaml


Then, you need these two other directories:

   /data/trec-kba/keys ---- from the trec-kba-secret-keys.tar.gz that is in the Dropbox
   /data/trec-kba/third/lingpipe-4.1.0 --- also in the dropbox




Wikipedia Links Corpus (WLC)
----------------------------

First step: To make use of the WLC data released by Google, we
transform the original inputs into 10888934 streamcorpus objects, and
grab the raw from the 10833242 thrift messages provided by UMass Brian
Martin's initial fetch.  This is implemented in
trec-kba-data/import_wlc.py, which is run in Condor by
run_import_wlc.{sh,submit}

