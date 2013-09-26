TREC KBA Data
=============

kba.pipeline is a document processing pipeline that assembles
streamcorpus objects from raw data sets for us in TREC KBA.

TREC KBA 2013
-------------
1006305073 all-stream-ids.suc.txt
884018982 all-stream-ids.doc_ids.suc.txt


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



installation
------------

Easiest to put this entire repo at a path like

    /data/trec-kba/installs/trec-kba-data

which is hardcoded into these three files:

    scripts/spinn3r-transform.sh
    scripts/spinn3r-transform.submit
    configs/spinn3r-transform.yaml


Then, you need these two other directories:

   /data/trec-kba/keys ---- from the trec-kba-secret-keys.tar.gz that is in the Dropbox
   /data/trec-kba/third/lingpipe-4.1.0 --- also in the dropbox

As a test run this:

    ## first go inside the virtualenv
    source /data/trec-kba/installs/py27env/bin/activate

    ## install all the python libraries
    make install

    ## run a simple test
    make john-smith-simple


and if that works, then try
    
    make john-smith


To try doing the real pull/push from AWS, you can put the input paths here:
   /data/trec-kba/installs/trec-kba-data/spinn3r-transform
   zcat spinn3r-transform-input-paths.txt.gz | split -l 150 -a 4
   b=0; for a in `ls ?????`; do mv $a input.$b; let b=$b+1; done;

and then locally as a test:

   cat  /data/trec-kba/installs/trec-kba-data/spinn3r-transform/input.0 | python -m kba.pipeline.run configs/spinn3r-transform.yaml

and then, after seeing that work edit the submit script to have as
many jobs as their are input files:

   condor_submit scripts/spinn3r-transform.submit

There is one key problem with this, which we discussed on the phone:
when the job dies, it starts over on the input list.  Let's discuss
using the zookeeper "task_queue" stage.


running on task_queue: zookeeper 
--------------------------------

To use the zookeeper task queue, you must install zookeeper on a
computer that your cluster can access.  Here is an example zookeeper
config:

# The number of milliseconds of each tick
tickTime=10000
# The number of ticks that the initial 
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
dataDir=/var/zookeeper
# the port at which the clients will connect
clientPort=2181
server.0=localhost:2888:3888
maxClientCnxns=2000


Note the large maxClientCnxns for running with many nodes in condor,
and also not the 10sec tickTime, which is needed to avoid frequent
session timeouts from condor slots that are working hard.


To make a job run off the zookeeper task queue, make these changes:

configs/spinn3r-transform.yaml:

-  task_queue: stdin
+  #task_queue: stdin
+
+  task_queue: zookeeper
+  zookeeper:
+    namespace: spinn3r-transform
+    zookeeper_address: mitas-2.csail.mit.edu:2181
 

scripts/spinn3r-transform.submit:

-Input  = /data/trec-kba/spinn3r-transform/input.$(PROCESS)
+
+## disable stdin because we are using task_queue: zookeeper 
+#Input = /data/trec-kba/spinn3r-transform/input.$(PROCESS)


Important:
Also update the number of jobs at the end of the .submit file.


and then  do these steps on the command line:

  ## see the help text
  python -m kba.pipeline.load configs/spinn3r-transform.yaml -h

  ## load the data
  python -m kba.pipeline.load configs/spinn3r-transform.yaml --load spinn3r-transform-input-paths.txt 

  ## check the counts -- might take a bit to run, so background and come back to it
  python -m kba.pipeline.load configs/spinn3r-transform.yaml --counts >& counts &

  ## launch the jobs
  condor_submit scripts/spinn3r-transform.submit 

  ## watch the logs for the jobs
  tail -f ../spinn3r-transform/{err,out}*


Periodically check the --counts on the queue and see how fast it is
going.  Do we need to turn off the lingpipe stage?

