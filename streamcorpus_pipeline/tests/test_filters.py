
from streamcorpus import make_stream_item
from streamcorpus_pipeline._filters import filter_domains, domain_name_cleanse, domain_name_left_cuts

def test_filter_domains(tmpdir):

    domains_path = tmpdir.join('domains_path.txt')
    domains_path.write('cats.com\nhttp://birds.com/')
    
    stage = filter_domains(dict(
            include_domains = ['dogs.com'],
            include_domains_path = str(domains_path),
            ))

    assert stage.domains == set(['dogs.com', 'cats.com', 'birds.com'])

    si = make_stream_item(0, 'http://dogs.com/')
    assert stage(si) is not None

    si = make_stream_item(0, 'http://cats.com/')
    assert stage(si) is not None

    si = make_stream_item(0, 'http://birds.com/')
    assert stage(si) is not None

    si = make_stream_item(0, 'http://things.com/')
    assert stage(si) is None

    si = make_stream_item(0, 'http://things.com/')
    si.schost = 'https://birds.com'
    assert domain_name_cleanse(si.schost) == 'birds.com'
    assert stage(si) is not None

def test_domain_name_left_cuts():
    assert domain_name_left_cuts('www.5.cars.com') == ['www.5.cars.com', '5.cars.com', 'cars.com', 'com']
