
clean_js:
	rm -f data/john-smith/john-smith-0.sc

john-smith: clean_js clean install
	echo data/john-smith/original | python -m kba.pipeline.run configs/john-smith.yaml
	echo data/john-smith/original | python -m kba.pipeline.run configs/john-smith-lingpipe.yaml
	echo data/john-smith/john-smith-0.sc | python -m kba.pipeline.run configs/john-smith-lingpipe-from-chunk.yaml

	## figure out how to make tests from this
	##diff data/john-smith/john-smith-tagged-by-lingpipe-0.sc  data/john-smith/john-smith-tagged-by-lingpipe-test-0.sc

clean: 
	rm -rf build dist src/kba.pipeline.egg-info

install: clean
	python setup.py build
	python setup.py install

