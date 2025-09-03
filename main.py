import sys
import os
import logging
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from services.pdf_converter import PDFConverter
from services.document_provider import DocumentProvider
from process.pdf_parsing import PDFParsing
from process.table_processing import TableProcessing

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)], force=True)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting processing pipeline...")
    pdf_converter = PDFConverter(logger=logger, model_path=args.model_path, num_threads=args.accelerator_thread, image_resolution=4.0)
    document_provider = DocumentProvider(logger=logger)
    # 1. PDF Parsing
    pdf_conversion = PDFParsing(
        document_provider=document_provider,
        pdf_converter=pdf_converter,
        logger=logger,
        image_resolution=args.high_res_table_resolution
    )
    logger.info("Processing pipeline finished successfully.")

if __name__ == "__main__":
    main()