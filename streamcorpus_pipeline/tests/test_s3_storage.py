from __future__ import absolute_import, division, print_function
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from functools import partial
import hashlib
import os
import os.path as path
import tarfile
import tempfile
import urllib2
import uuid

import pytest
from backports import lzma

from kvlayer.instance_collection import Chunk as ICChunk
try:
    from dossier.fc import FeatureCollection
except ImportError:
    pass
import yakonfig

import streamcorpus
from streamcorpus_pipeline._exceptions import FailedExtraction
import streamcorpus_pipeline._s3_storage as s3stage


# Skip all tests in this module if the AWS credentials are not in the
# environment.
pytestmark = pytest.mark.skipif(
    not os.getenv('AWS_ACCESS_KEY_ID'),
    reason='Requires AWS keys in environment. Set the following: '
           'AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_TEST_BUCKET.')


@pytest.fixture(scope='function')
def bucket_name():
    return os.getenv('AWS_TEST_BUCKET') or 'diffeo-test'


@pytest.fixture(scope='function')
def bucket(request, bucket_name):
    assert os.getenv('AWS_ACCESS_KEY_ID')
    assert os.getenv('AWS_SECRET_ACCESS_KEY')

    # Reconnect here because Amazon's keep-alive is wicked short and
    # boto won't *reliably* reconnect for us.
    # See: https://github.com/boto/boto/issues/1934
    # def close(): 
        # b = s3stage.get_bucket({'bucket': bucket_name}) 
        # b.delete_keys(list(b.list())) 
    # request.addfinalizer(close) 

    return s3stage.get_bucket({'bucket': bucket_name})


def clean_bucket(name, prefix):
    b = s3stage.get_bucket({'bucket': name})
    b.delete_keys(list(b.list(prefix=prefix)))


@pytest.yield_fixture
def from_s3_chunks(bucket_name):
    testc = {
        'from_s3_chunks': {
            'bucket': bucket_name,
            'tries': 1,
            's3_path_prefix': uuid.uuid4().hex,
        },
    }
    defconf = yakonfig.defaulted_config([s3stage.from_s3_chunks], config=testc)
    with defconf as conf:
        yield s3stage.from_s3_chunks(conf['from_s3_chunks'])
        clean_bucket(bucket_name, testc['from_s3_chunks']['s3_path_prefix'])


@pytest.yield_fixture
def to_s3_chunks(bucket_name):
    testc = {
        'to_s3_chunks': {
            'bucket': bucket_name,
            'output_name': '%(input_fname)s',
            'tries': 1,
            # Disable all verification so we can
            # selectively enable it in tests.
            'verify': False,
            'is_private': False,
            'tarinfo_name': '%(stream_id)s',
            'tarinfo_uname': 'utartest',
            'tarinfo_gname': 'gtartest',
            's3_path_prefix': uuid.uuid4().hex,
        },
    }
    defconf = yakonfig.defaulted_config([s3stage.to_s3_chunks], config=testc)
    with defconf as conf:
        yield s3stage.to_s3_chunks(conf['to_s3_chunks'])
        clean_bucket(bucket_name, testc['to_s3_chunks']['s3_path_prefix'])


@pytest.fixture
def to_s3_tarballs(to_s3_chunks):
    return s3stage.to_s3_tarballs(to_s3_chunks.config)


def run_to_s3(to_s3_chunks, keydata):
    pfx = 'tmp_%s_' % keydata['key']
    # We don't delete it because `to_s3_storage` will do it.
    tmp = tempfile.NamedTemporaryFile(mode='w+b', prefix=pfx,
                                      delete=False)
    with tmp as tmpf:
        tmpf.write(keydata['raw'])
        tmpf.flush()
        to_s3_chunks(tmpf.name, {'num': 1}, keydata['key'])


def md5_key(k, data):
    return k + '-' + hashlib.md5(data).hexdigest()


def make_dummy_chunks(dummy_chunks, chunker, make_item, md5=False):
    chunks = {}
    for chunk_key_name, sinames in dummy_chunks.items():
        cdata = StringIO()
        with chunker(file_obj=cdata, mode='wb') as c:
            for n in sinames:
                c.add(make_item(n))
            c.flush()

            errs, data = streamcorpus.compress_and_encrypt(cdata.getvalue())
            assert len(errs) == 0

            if md5:
                # The md5 is on the uncompressed and unencrypted data.
                new_key = md5_key(chunk_key_name, cdata.getvalue())
            else:
                new_key = chunk_key_name
            chunks[chunk_key_name] = {
                    'key': new_key, 'data': data, 'raw': cdata.getvalue(),
            }
    return chunks


def make_dummy_si_chunks(dummy_chunks, md5=False):
    def make(name):
        si = streamcorpus.make_stream_item('2000-01-01T12:34:00.000123Z',
                                           name)
        si.stream_id = name
        si.body.clean_visible = 'clean visible test text'
        return si
    return make_dummy_chunks(dummy_chunks, streamcorpus.Chunk, make, md5=md5)


def make_dummy_fc_chunks(dummy_chunks, md5=False):
    def make(name):
        fc = FeatureCollection()
        fc['canonical_name'][name] = 1
        return fc
    return make_dummy_chunks(dummy_chunks, ICChunk, make, md5=md5)


# Avoid testing feature collections when dossier is not present.
# TODO: Remove this when dossier is released.
formats = ['streamitem']
if 'FeatureCollection' in globals():
    formats.append('featurecollection')
@pytest.fixture(params=formats)
def chunker(request):
    if request.param == 'streamitem':
        make_chunks = make_dummy_si_chunks
    else:
        make_chunks = make_dummy_fc_chunks
    return {'make_chunks': make_chunks, 'format': request.param}


def put_chunks(bucket, chunks, prefix=''):
    # Used for putting chunks before getting them with from_s3_storage.
    for info in chunks.values():
        k = bucket.new_key(path.join(prefix, info['key']))
        k.set_contents_from_string(info['data'])


def get_chunks(bucket, chunks, format, prefix=''):
    # Used for fetching chunks after putting them with to_s3_storage.
    ext = 'fc' if format.lower() == 'featurecollection' else 'sc'
    datas = {}
    for chunk_key_name, info in chunks.items():
        k = bucket.new_key(path.join(prefix, '%s.%s.xz' % (info['key'], ext)))
        raw = k.get_contents_as_string()
        errs, datas[chunk_key_name] = streamcorpus.decrypt_and_uncompress(raw)
        assert len(errs) == 0
    return datas


def public_url(bucket_name, key, format, prefix=''):
    ext = 'fc' if format.lower() == 'featurecollection' else 'sc'
    u = 'https://s3.amazonaws.com/%s/' % bucket_name
    return path.join(u, prefix, '%s.%s.xz' % (key, ext))


def read_chunk(chunk_data, format):
    fc = format.lower() == 'featurecollection'
    chunk_type = ICChunk if fc else streamcorpus.Chunk
    return chunk_type(data=chunk_data)


def get_chunk_items(bucket, chunks, format, prefix=''):
    read = get_chunks(bucket, chunks, format, prefix=prefix)
    for cdata in read.values():
        for it in read_chunk(cdata, format):
            yield it


def name_from_item(item):
    if isinstance(item, streamcorpus.StreamItem):
        return item.abs_url
    else:
        return item['canonical_name'].keys()[0]


def test_to_s3_chunks_count(bucket, to_s3_chunks, chunker):
    dummies = {'a': ['a1'], 'b': ['b1', 'b2'], 'c': ['c1']}
    chunks = chunker['make_chunks'](dummies)
    to_s3_chunks.config['output_format'] = chunker['format']
    map(partial(run_to_s3, to_s3_chunks), chunks.values())

    its = get_chunk_items(bucket, chunks, chunker['format'],
                          prefix=to_s3_chunks.config['s3_path_prefix'])
    assert 4 == len(list(its))


def test_to_s3_tarballs_count(bucket, to_s3_tarballs, chunker):
    if chunker['format'].lower() != 'streamitem':
        pytest.skip('to_s3_tarballs only works with stream items')

    dummies = {'a': ['a1'], 'b': ['b1', 'b2'], 'c': ['c1']}
    chunks = chunker['make_chunks'](dummies)
    to_s3_tarballs.config['output_format'] = chunker['format']
    map(partial(run_to_s3, to_s3_tarballs), chunks.values())

    for key in dummies:
        fullkey = path.join(to_s3_tarballs.config['s3_path_prefix'],
                            '%s.tar.gz' % key)
        data = bucket.get_key(fullkey).get_contents_as_string()
        tar = tarfile.open(fileobj=StringIO(data), mode='r:gz')
        assert sorted(dummies[key]) == sorted(tar.getnames())


def test_from_s3_chunks_count(bucket, from_s3_chunks, chunker):
    dummies = {'a': ['a1'], 'b': ['b1', 'b2'], 'c': ['c1']}
    from_s3_chunks.config['input_format'] = chunker['format']
    put_chunks(bucket, chunker['make_chunks'](dummies),
               prefix=from_s3_chunks.config['s3_path_prefix'])
    assert 4 == sum(len(list(from_s3_chunks(k))) for k in dummies)


def test_to_s3_chunks_read(bucket, to_s3_chunks, chunker):
    dummies = {'a': ['a1'], 'b': ['b1', 'b2'], 'c': ['c1']}
    chunks = chunker['make_chunks'](dummies)
    to_s3_chunks.config['output_format'] = chunker['format']
    map(partial(run_to_s3, to_s3_chunks), chunks.values())

    read_chunks = get_chunks(bucket, chunks, chunker['format'],
                             prefix=to_s3_chunks.config['s3_path_prefix'])
    for key, sinames in dummies.items():
        sinames = sorted(sinames)
        got_names = [name_from_item(it)
                     for it in read_chunk(read_chunks[key], chunker['format'])]
        assert sinames == got_names


def test_from_s3_chunks_read(bucket, from_s3_chunks, chunker):
    dummies = {'a': ['a1'], 'b': ['b1', 'b2'], 'c': ['c1']}
    from_s3_chunks.config['input_format'] = chunker['format']
    put_chunks(bucket, chunker['make_chunks'](dummies),
               prefix=from_s3_chunks.config['s3_path_prefix'])
    for key, sinames in dummies.items():
        sinames = sorted(sinames)
        got_names = [name_from_item(it) for it in from_s3_chunks(key)]
        assert sinames == got_names


def test_from_s3_si_chunks_md5(bucket, from_s3_chunks, chunker):
    dummies = {'a': ['a1'], 'b': ['b1', 'b2'], 'c': ['c1']}
    from_s3_chunks.config['input_format'] = chunker['format']
    chunks = chunker['make_chunks'](dummies, md5=True)
    put_chunks(bucket, chunks, prefix=from_s3_chunks.config['s3_path_prefix'])

    from_s3_chunks.config['compare_md5_in_file_name'] = True
    assert 4 == sum(len(list(from_s3_chunks(chunks[k]['key'])))
                    for k in dummies)


def test_from_s3_chunks_md5_invalid(bucket, from_s3_chunks, chunker):
    # This tests that a proper FailedExtraction is raised if the config
    # requests for md5 verification when the key doesn't have one in its
    # name.
    dummies = {'a': ['a1']}
    from_s3_chunks.config['input_format'] = chunker['format']
    chunks = chunker['make_chunks'](dummies, md5=False)
    put_chunks(bucket, chunks, prefix=from_s3_chunks.config['s3_path_prefix'])
    from_s3_chunks.config['compare_md5_in_file_name'] = True
    with pytest.raises(FailedExtraction):
        from_s3_chunks(chunks['a']['key'])


def test_from_s3_chunks_md5_corrupt(bucket, from_s3_chunks, chunker):
    dummies = {'a': ['a1']}
    chunks = chunker['make_chunks'](dummies, md5=True)
    put_chunks(bucket, chunks, prefix=from_s3_chunks.config['s3_path_prefix'])

    from_s3_chunks.config['input_format'] = chunker['format']
    from_s3_chunks.config['compare_md5_in_file_name'] = True

    # Corrupt the data so the md5 won't match.
    fullkey = path.join(from_s3_chunks.config['s3_path_prefix'],
                        chunks['a']['key'])
    key = bucket.get_key(fullkey)
    key.set_contents_from_string(lzma.compress('FUBAR'))

    with pytest.raises(FailedExtraction):
        from_s3_chunks(chunks['a']['key'])


def test_from_s3_chunks_bad_informat(bucket, from_s3_chunks, chunker):
    dummies = {'a': ['a1']}
    from_s3_chunks.config['input_format'] = chunker['format']
    put_chunks(bucket, chunker['make_chunks'](dummies),
               prefix=from_s3_chunks.config['s3_path_prefix'])

    from_s3_chunks.config['input_format'] = 'FUBAR'
    with pytest.raises(FailedExtraction):
        from_s3_chunks('a')


def test_to_s3_chunks_bad_outformat(bucket, to_s3_chunks, chunker):
    dummies = {'a': ['a1']}
    chunks = chunker['make_chunks'](dummies)

    to_s3_chunks.config['output_format'] = 'FUBAR'
    with pytest.raises(FailedExtraction):
        run_to_s3(to_s3_chunks, chunks['a'])


def test_to_s3_chunks_verify(bucket, to_s3_chunks, chunker):
    # This is a pretty hokey test. It basically just tests that
    # the 'verify' config parameter won't result in a failure.
    # Ideally, we could write a test that causes verification to
    # fail, but I don't know the best way to do that.
    dummies = {'a': ['a1'], 'b': ['b1', 'b2'], 'c': ['c1']}
    chunks = chunker['make_chunks'](dummies)
    to_s3_chunks.config['output_format'] = chunker['format']
    to_s3_chunks.config['verify'] = True
    map(partial(run_to_s3, to_s3_chunks), chunks.values())

    its = get_chunk_items(bucket, chunks, chunker['format'],
                          prefix=to_s3_chunks.config['s3_path_prefix'])
    assert 4 == len(list(its))


def test_to_s3_chunks_public(bucket_name, bucket, to_s3_chunks, chunker):
    dummies = {'a': ['a1']}
    chunks = chunker['make_chunks'](dummies)
    to_s3_chunks.config['output_format'] = chunker['format']
    to_s3_chunks.config['is_private'] = False

    run_to_s3(to_s3_chunks, chunks['a'])
    purl = public_url(bucket_name, 'a', chunker['format'],
                      prefix=to_s3_chunks.config['s3_path_prefix'])
    urllib2.urlopen(purl)


def test_to_s3_chunks_private(bucket_name, bucket, to_s3_chunks, chunker):
    dummies = {'a': ['a1']}
    chunks = chunker['make_chunks'](dummies)
    to_s3_chunks.config['output_format'] = chunker['format']
    to_s3_chunks.config['is_private'] = True

    run_to_s3(to_s3_chunks, chunks['a'])
    try:
        purl = public_url(bucket_name, 'a', chunker['format'],
                          prefix=to_s3_chunks.config['s3_path_prefix'])
        urllib2.urlopen(purl)
    except urllib2.HTTPError as e:
        assert e.code == 403
    else:
        raise Exception('Expected to get a 403 Forbidden when accessing '
                        'private S3 file.')
