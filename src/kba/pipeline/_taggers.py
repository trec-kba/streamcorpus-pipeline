

import os
import exceptions

from streamcorpus import Chunk
from ._clean_visible import make_clean_visible_file

class TaggerBatchTransform(object):
    '''
    kba.pipeline.TaggerBatchTransform provides a structure for
    aligning a taggers output with labels and generating
    stream_item.sentences[tagger_id] = [Sentence]
    '''
    def __init__(self, config):
        self.config = config

    def __call__(self, chunk_path):
        ## make temporary file paths based on chunk_path
        clean_visible_path = chunk_path + '-clean_visible.xml'
        ner_xml_path       = chunk_path + '-ner.xml'

        ## process the chunk's clean_visible data into xml
        i_chunk = Chunk(path=chunk_path, mode='rb')
        make_clean_visible_file(i_chunk, clean_visible_path)

        ## generate LingPipe output
        self.make_ner_file(clean_visible_path, ner_xml_path)

        ## make a new output chunk at a temporary path
        tmp_chunk_path     = chunk_path + '_'
        o_chunk = Chunk(path=tmp_chunk_path, mode='wb')

        ## fuse the NER with i_chunk to make o_chunk
        self.align_chunk_with_ner(ner_xml_path, i_chunk, o_chunk)

        ## clean up temp files
        os.remove(clean_visible_path)
        os.remove(ner_xml_path)

        ## atomic rename new chunk file into place
        os.rename(tmp_chunk_path, chunk_path)

    def make_ner_file(self, clean_visible_path, ner_xml_path):
        raise exceptions.NotImplementedError

    def align_chunk_with_ner(ner_xml_path, i_chunk, o_chunk):
        raise exceptions.NotImplementedError

