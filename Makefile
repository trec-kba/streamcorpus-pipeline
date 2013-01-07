
clean_js:
	rm -f data/john-smith/john-smith-0.sc

john-smith: clean_js clean install
	echo data/john-smith/original | python -m kba.pipeline.run configs/john-smith.yaml
	echo data/john-smith/original | python -m kba.pipeline.run configs/john-smith-lingpipe.yaml

clean: 
	rm -rf build dist src/kba.pipeline.egg-info

install: clean
	python setup.py build
	python setup.py install

