'''tests for streamcorpus_pipeline.rmtree

.. This software is released under an MIT/X11 open source license.
   Copyright 2014 Diffeo, Inc.
'''
import logging
import os
import pytest

from streamcorpus_pipeline.util import rmtree

#logging.basicConfig()
logger = logging.getLogger()

@pytest.mark.parametrize('use_shutil', [True, False])
def test_rmtree_basic(tmpdir, use_shutil):
    path1 = tmpdir.join('a/b/c/d')
    os.makedirs(os.path.dirname(str(path1)))
    path1.write('test')

    path2 = tmpdir.join('a/c/d')
    os.makedirs(os.path.dirname(str(path2)))
    path2.write('test')

    observed = set()
    for root, dir_names, file_names in os.walk(str(tmpdir)):
        for fname in file_names:
            fpath = os.path.join(root, fname)
            observed.add(fpath)

            ## set the permissions so that nobody can even read it,
            ## and it should still get deleted.
            os.chmod(fpath, 0000)

    assert len(observed) == 2

    logger.info(os.listdir(str(tmpdir)))

    rmtree(str(tmpdir.join('a')), use_shutil=use_shutil)

    logger.info(os.listdir(str(tmpdir)))

    assert  len(os.listdir(str(tmpdir))) == 0


@pytest.mark.parametrize('use_shutil', [True, False])
def test_rmtree_single_file(tmpdir, use_shutil):
    path1 = tmpdir.join('b/c')
    dirname = os.path.dirname(str(path1))
    os.makedirs(dirname)
    path1.write('test')

    os.chmod(str(path1), 0000)

    assert len(os.listdir(dirname)) == 1

    logger.info(os.listdir(dirname))

    rmtree(str(tmpdir.join('b/c')), use_shutil=use_shutil)

    logger.info(os.listdir(dirname))

    assert  len(os.listdir(dirname)) == 0


def make_tree_with_symlink(tmpdir):
    path1 = tmpdir.join('c/c/d/e')
    os.makedirs(os.path.dirname(str(path1)))
    path1.write('test')

    os.makedirs(str(tmpdir.join('c/d')))
    os.symlink(str(tmpdir.join('c/c')), str(tmpdir.join('c/d/link')))

    path_to_delete = str(tmpdir.join('c/d'))

    observed = set()
    for root, dir_names, file_names in os.walk(path_to_delete, followlinks=True):
        for fname in file_names:
            fpath = os.path.join(root, fname)
            observed.add(fpath)

            ## set the permissions so that nobody can even read it,
            ## and it should still get deleted.
            os.chmod(fpath, 0000)

    assert len(observed) == 1

    dirname = str(tmpdir.join('c'))
    assert len(os.listdir(dirname)) == 2

    logger.info(os.listdir(dirname))

    return path_to_delete

def test_rmtree_followlinks_True(tmpdir):
    path_to_delete = make_tree_with_symlink(tmpdir)
    rmtree(path_to_delete, followlinks=True)

    observed = set()
    for root, dir_names, file_names in os.walk(str(tmpdir.join('c')), followlinks=True):
        for fname in dir_names + file_names:
            fpath = os.path.join(root, fname)
            observed.add(fpath)

    logger.info('\n'.join(sorted(observed)))
    assert  len(observed) == 0

def test_rmtree_followlinks_False(tmpdir):
    path_to_delete = make_tree_with_symlink(tmpdir)
    rmtree(path_to_delete, followlinks=False)

    observed = set()
    for root, dir_names, file_names in os.walk(str(tmpdir.join('c')), followlinks=True):
        for fname in dir_names + file_names:
            fpath = os.path.join(root, fname)
            observed.add(fpath)

    logger.info('\n'.join(sorted(observed)))

    logger.info(observed)

    assert  len(observed) == 3
