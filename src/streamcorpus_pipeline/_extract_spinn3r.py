'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import sys
import zlib
import json
import traceback
from streamcorpus import make_stream_item, Chunk, ContentItem
from _spinn3r import spinn3rApi_pb2
from _spinn3r import protoStream_pb2

def log(msg):
    print(msg)

def _VarintDecoder(mask):
    '''
    This is lifted out of the internals of Google protobuf library,
    and is needed to deserialize concatenated protobuf messages in the
    custom protostream format that spinn3r provides.

    Like _VarintDecoder() but decodes signed values.
    '''
    local_ord = ord
    def DecodeVarint(buffer, pos):
        result = 0
        shift = 0
        while 1:
            b = local_ord(buffer[pos])
            result |= ((b & 0x7f) << shift)
            pos += 1
            if not (b & 0x80):
                if result > 0x7fffffffffffffff:
                    result -= (1 << 64)
                    result |= ~mask
                else:
                    result &= mask
                    return (result, pos)
            shift += 7
            if shift >= 64:
                ## should create this exception and also catch it below
                raise ValueError('Too many bytes when decoding varint.')
    return DecodeVarint

def delimited_messages(data):
    '''
    Provides iteration over entry_instance, delimited_entry_bytes
    '''
    ## get a 64bit varint decoder
    decoder = _VarintDecoder((1<<64) - 1)

    ## get two of the three types of protobuf messages we expect.
    ## third one (entry) we create below.
    header    = protoStream_pb2.ProtoStreamHeader()
    delimiter = protoStream_pb2.ProtoStreamDelimiter()

    num_bytes = len(data)

    try:
        ## get the header
        pos = 0
        pre_decoder_pos = pos
        delta, pos = decoder(data, pos)
        header.ParseFromString(data[pos:pos + delta])

        yield header, data[pre_decoder_pos : pos + delta]

        while 1:
            pos += delta
            pre_decoder_pos = pos
            try:
                delta, pos = decoder(data, pos)
                delimiter.ParseFromString(data[pos:pos + delta])
            except Exception, exc:
                log(traceback.format_exc(exc))
                log('giving up on this protostream file')
                yield None, ''
                return
            
            #print pos

            if delimiter.delimiter_type == delimiter.END:  # pylint: disable=E1101
                ## last yield provides None as a flag and the bytes of
                ## the end delimiter
                yield None, data[pre_decoder_pos : pos + delta]
                return

            pos += delta
            delta, pos = decoder(data, pos)

            ## create a new entry to yield to the caller
            entry = spinn3rApi_pb2.Entry()
            try:
                entry.ParseFromString(data[pos:pos + delta])
            except Exception, exc:
                log(traceback.format_exc(exc))
                log('giving up on this protostream file')
                yield None, ''
                return

            ## yield the entry object and also the byte array
            ## containing the delimiter message and the entry message
            yield entry, data[pre_decoder_pos : pos + delta]

    except Exception, exc:
        log('broken data:\n%s' % traceback.format_exc(exc))
    finally:
        try:
            if num_bytes != pos + delta:
                log('%d = num_bytes != pos + delta = %d' % (num_bytes, pos + delta))
        except Exception, exc:
            log(str(exc))
            log('continuing')



def _generate_stream_items(protobuf_data):
    '''
    converts all of the protobuf_data spinn3r protoStream format into
    StreamItems, which it yields as a generator
    '''
    ## iterate over entry objects and bytes from which they came
    for num, (entry, delimited_bytes) in enumerate(delimited_messages(protobuf_data)):
        #print num
        if entry is None:
            ## hit end of data
            continue

        if not hasattr(entry, 'permalink_entry'):
            #print 'missing permalink_entry'
            continue

        pe = entry.permalink_entry

        ## verify our understanding of the kludgy link data
        #assert pe.link[0].href[:len(pe.link[0].resource)] == pe.link[0].resource, \
        #    (pe.link[0].href, pe.link[0].resource)
        #assert pe.link[0].href == pe.canonical_link.href
        #assert pe.canonical_link.href.startswith(pe.canonical_link.resource), \
        #    (pe.canonical_link.href, pe.canonical_link.resource)

        ## create a StreamItem for this date_found, canonical_link
        si = make_stream_item(
            pe.date_found[:-1] + '.0Z',
            pe.canonical_link.href.encode('utf8'))

        if not si.stream_time:
            print 'failed to generate stream_time from: %r' % pe.date_found
            continue

        if not si.abs_url:
            print 'failed to generate abs_url from: %r' % pe.canonical_link.href
            continue

        if not pe.content.data:
            continue

        try:
            raw = zlib.decompress(pe.content.data)
            assert raw
        except Exception, exc:
            #print('failed to get anything from decompressing pe.content.data')
            try:
                raw = zlib.decompress(entry.feed_entry.content.data)
                assert raw
            except Exception, exc:
                #print('failed to get anything from decompressing entry.feed_entry.content.data')
                #print('empty entry? %s' % entry)
                continue

        si.body=ContentItem(
            raw = raw.decode('utf8').encode('utf8'),
            media_type = pe.content.mime_type,
            )

        ## be really sure not to emit an invalid StreamItem
        if not si.body:
            continue
        if not si.body.raw:
            continue

        si.other_content['extract'] = ContentItem(
            raw = zlib.decompress(pe.content_extract.data).decode('utf8').encode('utf8'),
            media_type = pe.content_extract.mime_type,
            )

        si.other_content['title'] = ContentItem(
            ## is this really in unicode already?
            raw = pe.title.encode('utf8'),
            media_type = pe.content_extract.mime_type,
            encoding = 'UTF-8',
            )

        si.other_content['feed_entry_title'] = ContentItem(
            ## is this really in unicode already?
            raw = entry.feed_entry.title.encode('utf8'),
            media_type = entry.feed_entry.content.mime_type,
            encoding = 'UTF-8',
            )

        if entry.feed_entry.content.data:
            si.other_content['feed_entry'] = ContentItem(
                raw = zlib.decompress(entry.feed_entry.content.data),
                media_type = entry.feed_entry.content.mime_type,
                )

        si.source_metadata['lang'] = pe.lang[0].code
        si.source_metadata['author'] = json.dumps( 
            dict(
                name = pe.author[0].name,
                email = pe.author[0].email,
                link = pe.author[0].link[0].href,
                )
            )
        si.source = entry.source.publisher_type

        yield si


if __name__ == '__main__':
    #import sys
    #from _handle_unconvertible_spinn3r import handle_unconvertible_spinn3r as hus
    #map(hus, _generate_stream_items(sys.stdin.read()))

    o_chunk = Chunk('/tmp/foo.sc', mode='wb')
    for si in _generate_stream_items(sys.stdin.read()):
        print '---post smoosh raw: %s --' % si.stream_id
        print si.body.raw
        print si.stream_id

        if si.stream_id == '1345928297-da71cfa833ce8218684b6dab152dd69b':
            o_chunk.add( si )

    o_chunk.close()
