import sys
import os
import logging
from pathlib import Path
import ops_logging


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.arg_parser import get_args
from core.orchestrator import PipelineOrchestrator

from service_object.pdf_converter import PDFConverter
from service_object.document_provider import DocumentProvider
from service_object.vlm_table_processor import VLMTableProcessor
from service_object.content_structurer import ContentStructurer

from process.pdf_parsing import PDFParsing
from process.table_processing import TableProcessing
from process.content_structuring import ContentStructuring

# ops_logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
#                     handlers=[logging.StreamHandler(sys.stdout)], force=True)

logger = ops_logging.get_logger("service")

def main():
    logger.info("Starting processing pipeline...")
    args = get_args()
    
    pdf_converter = PDFConverter(logger=logger, model_path=args.model_path, num_threads=args.accelerator_thread, image_resolution=4.0)
    document_provider = DocumentProvider(logger=logger)
    vlm_processor = VLMTableProcessor(logger=logger, base_url=args.vlm_base_url, model_name=args.vlm_model_name, concurrency_limit=args.vlm_concurrency_limit)
    content_structurer = ContentStructurer(logger=logger)

    # 1. PDF Parsing
    pdf_conversion = PDFParsing(
        document_provider=document_provider,
        pdf_converter=pdf_converter,
        logger=logger,
        image_resolution=args.high_res_table_resolution
    )

    # 2. Table Processing
    vlm_table_processing = TableProcessing(
        vlm_processor=vlm_processor,
        logger=logger
    )
    
    # 3. Content Structuring
    content_structuring = ContentStructuring(
        document_provider=document_provider,
        content_structurer=content_structurer,
        logger=logger
    )

    orchestrator = PipelineOrchestrator(steps=[pdf_conversion, vlm_table_processing, content_structuring])
    
    initial_context = {
        "output_dir": Path(args.output_dir),
        "file_list_path": Path(args.file_list_path),
    }
    
    orchestrator.run(initial_context)

    logger.info("Processing pipeline finished successfully.")

if __name__ == "__main__":
    main()