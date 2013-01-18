
clean_js:
	## to make real tests, we need to dump a non-time-sensitive
	## extract from these Chunk files and compare it to a stored
	## copy of what is expected.
	git checkout -- data/john-smith/john-smith-0.sc
	git checkout -- data/john-smith/john-smith-tagged-by-lingpipe-0.sc
	rm -f data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc
	rm -rf tmp

john-smith: clean_js clean dev-all
	echo data/john-smith/original | python -m kba.pipeline.run configs/john-smith.yaml
	echo data/john-smith/original | python -m kba.pipeline.run configs/john-smith-lingpipe.yaml
	echo data/john-smith/john-smith-0.sc | python -m kba.pipeline.run configs/john-smith-lingpipe-from-chunk.yaml

	## compare dumps
	python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-lingpipe-0.sc      --tokens > tmp/lp-0.tsv
	python -m streamcorpus.dump data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc --tokens > tmp/lp-test-0.tsv

	diff tmp/lp-0.tsv tmp/lp-test-0.tsv
	diff tmp/lp-0.tsv data/john-smith/john-smith-tagged-by-lingpipe-0.tsv

clean: clean_js
	rm -rf build dist src/kba.pipeline.egg-info

.IGNORE: lxml
lxml:
	## this should be done by cloudinit/puppet or something along
	## those lines
	#sudo apt-get -y install libxml2-dev libxslt-dev  

update_modules:
	## get the other submodules:  bigtree, kba-corpus, and third
	git submodule init

	## This clones the repositories and checks out the commits
	## specified by the superproject.
	git submodule update

	## This pulls in the HEAD of each of the submodules.  Maybe
	## should commit and tag a release here instead of checking
	## out master and pulling from HEAD.
	git submodule foreach git pull origin master

        ## To protect against the possibility that someone jumps into
        ## one of these dirs to make a change and does it not on any
        ## branch, for now we switch to master.  Perhaps we should
        ## split this into a dev-mode target and a release-mode
        ## target, where the latter clones readonly repos?
	git submodule foreach git checkout master

install-deps: update_modules
	cd streamcorpus && make install

install-kba: clean lxml
	python setup.py clean --all
	python setup.py build
	python setup.py install

dev-all: install-deps install-kba