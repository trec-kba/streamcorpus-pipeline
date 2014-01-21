import logging
from cStringIO import StringIO

from docx import Document

logger = logging.getLogger(__name__)

def extract_text(data):
    fp = StringIO(data)
    document = Document(fp)

    result = '\n\n'.join(para.text for para in document.paragraphs)

    fp.close()
    return result

def docx_to_text(config):
    '''
    returns a kba.pipeline "transform" function that attempts to
    generate stream_item.body.clean_visible from body.raw
    '''
    ## make a closure around config
    def _make_clean_visible(stream_item, context):

        if stream_item.body and stream_item.body.raw \
                and stream_item.body.media_type == 'application/msword':

            logger.debug('converting docx to text for %s',
                         stream_item.stream_id)

            try:
                stream_item.body.clean_visible = \
                    extract_text(stream_item.body.raw)
            except Exception as exc:
                logger.exception('failed to convert %s from docx',
                                 stream_item.stream_id)

        return stream_item

    return _make_clean_visible
