#!/bin/bash

export LD_LIBRARY_PATH=/data/trec-kba/installs/so

## if java is not in a normal place, then extend the path:
export PATH=$PATH:/bin:/usr/bin:/afs/csail/amd64_linux26/java/latest/bin

## setup our python virtualenv
source /data/trec-kba/installs/py27env/bin/activate

tries=0
while [ "$tries" -le "10" ]; do
  echo "attempting to run " `which python`
  python -c "import md5"

  ## if that succeeded, then break out... otherwise sleep and loop
  if [ "$?" = "0" ]; then
    echo "successfully loaded md5"
    break
  else
    echo $tries "runner failed with: " $?
    let tries=$tries+1
    sleep 4
  fi
done;

signal_handler()
{
	# print message
	#
	echo "Received signal..."

	pkill -P $$
        
	# Need to exit the script explicitly when done.
	# Otherwise the script would live on, until system
	# realy goes down, and KILL signals are send.
	#
	exit 0
}

trap 'signal_handler' SIGTERM SIGABRT SIGHUP SIGINT

## this will have to be modified for any other environments
python -c "import sys; assert sys.executable == '/data/trec-kba/installs/py27env/bin/python'"
if [ "$?" -ne "0" ]; then
  exit $?
else
  ## everything is determined by the config file passed in as $1
  python -m kba.pipeline.run  $1
fi
