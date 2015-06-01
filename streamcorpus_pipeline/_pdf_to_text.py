from __future__ import absolute_import
from cStringIO import StringIO
import logging

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.pdftypes import PDFException

from streamcorpus_pipeline.stages import Configured

logger = logging.getLogger(__name__)

## http://stackoverflow.com/questions/5725278/python-help-using-pdfminer-as-a-library
def convert_pdf_to_text(data):
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)

    pdfstr = StringIO(data)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos = set()
    for page in PDFPage.get_pages(pdfstr, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=True):
        interpreter.process_page(page)
    pdfstr.close()
    device.close()

    ret = retstr.getvalue()
    retstr.close()
    return ret

class pdf_to_text(Configured):
    '''
    returns a kba.pipeline "transform" function that attempts to
    generate stream_item.body.clean_visible from body.raw
    '''
    config_name = 'pdf_to_text'
    def __call__(self, stream_item, context):

        if stream_item.body and stream_item.body.raw \
                and stream_item.body.media_type == 'application/pdf':

            logger.debug('converting pdf to text for %s',
                         stream_item.stream_id)

            try:
                stream_item.body.clean_html = \
                    convert_pdf_to_text(stream_item.body.raw)
            except PDFException, exc:
                logger.info('dropping broken pdf file %s: %r',
                            exc, stream_item.abs_url)
                return None
            except Exception as exc:
                logger.exception('failed to convert %s from pdf',
                                 stream_item.stream_id)

        return stream_item
