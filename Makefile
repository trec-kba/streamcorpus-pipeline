
clean_js:
	rm -f data/john-smith/john-smith.sc

build_js:
	git submodule init
	git submodule update
	cd streamcorpus && make install
	python src/js_thriftify.py data/john-smith/original data/john-smith/john-smith.sc

clean: 
	rm -rf build dist src/kba.pipeline.egg-info

install: clean
	python setup.py build
	python setup.py install

