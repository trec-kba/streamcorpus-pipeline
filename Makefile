
clean_js:
	rm -f data/john-smith/john-smith.sc

build_js:
	git submodule init
	git submodule update
	cd streamcorpus && make install
	python src/js_thriftify.py data/john-smith/original data/john-smith/john-smith.sc

clean: clean_js

build: clean build_js