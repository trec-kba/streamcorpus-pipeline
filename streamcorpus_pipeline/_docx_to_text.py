from __future__ import absolute_import
import logging
from cStringIO import StringIO

from docx import Document
from zipfile import BadZipfile

from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

def extract_text(data):
    fp = StringIO(data)
    document = Document(fp)

    result = '\n\n'.join(para.text for para in document.paragraphs)

    fp.close()
    return result

class docx_to_text(Configured):
    '''
    returns a kba.pipeline "transform" function that attempts to
    generate stream_item.body.clean_visible from body.raw
    '''
    config_name = 'docx_to_text'
    def __call__(self, stream_item, context):
        if stream_item.body and stream_item.body.raw \
                and stream_item.body.media_type == 'application/msword':

            logger.debug('converting docx to text for %s',
                         stream_item.stream_id)

            try:
                stream_item.body.clean_html = \
                    extract_text(stream_item.body.raw)
            except BadZipfile, exc:
                logger.info('dropping broken docx file %s: %r',
                            exc, stream_item.abs_url)
                return None
            except Exception as exc:
                logger.exception('failed to convert %s from docx',
                                 stream_item.stream_id)

        return stream_item
