export PATH := /opt/diffeo/bin:$(PATH)

test: clean
	py.test --genscript=runtests.py
	python setup.py install_test > /dev/null
	## TODO: make `python setup.py test` do the same as the next
	## line, i.e. ensure that collectonly passes before running,
	## and runs in parallel
	cd src && py.test --collectonly && py.test -n 8


clean: 
	rm -rf build dist src/streamcorpus.pipeline.egg-info runtests.py *.iso

.IGNORE: lxml
lxml:
	## this should be done by cloudinit/puppet or something along
	## those lines
	#sudo apt-get -y install libxml2-dev libxslt-dev  

.PHONY : build
build: clean
	python setup.py bdist_egg sdist

post-build-test:
	echo data/john-smith/john-smith-0.sc | python -m streamcorpus.pipeline.run configs/john-smith-lingpipe-from-chunk.yaml

install: clean lxml
	## might need to do this on mac: export C_INCLUDE_PATH=/usr/include:/usr/local/include:/opt/local/include
	python setup.py install_test
	python setup.py clean --all
	python setup.py build
	python setup.py install

register:
        ## upload both source and binary
	python setup.py sdist bdist_egg upload -r internal

check:
	pylint -i y --output-format=parseable src/`git remote -v | grep origin | head -1 | cut -d':' -f 2 | cut -d'.' -f 1`

