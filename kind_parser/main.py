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
from service_object.db_uploader import DBUploader

from process.pdf_parsing import PDFParsing
from process.table_processing import TableProcessing
from process.content_structuring import ContentStructuring
from process.db_uploading import DBUploading

# ops_logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
#                     handlers=[logging.StreamHandler(sys.stdout)], force=True)

logger = ops_logging.get_logger("service")

def main():
    logger.info("Starting processing pipeline...")
    args = get_args()

    steps = []

    if 'pdf_conversion' in args.steps:
        logger.info(f"Process PDF Converting")

        pdf_converter_config = {
            "model_path": args.model_path,
            "num_threads": args.accelerator_thread,
            "image_resolution": args.image_resolution
        }
        document_provider = DocumentProvider(logger=logger)

        # 1. PDF Parsing
        pdf_conversion = PDFParsing(
            document_provider=document_provider,
            pdf_converter_config=pdf_converter_config,
            logger=logger,
            num_workers=args.pdf_parsing_num_workers
        )

        steps.append(pdf_conversion)

    if 'vlm_processing' in args.steps:
        logger.info(f"Process Table Parsing")

        vlm_processor = VLMTableProcessor(logger=logger, base_url=args.vlm_base_url, model_name=args.vlm_model_name, concurrency_limit=args.vlm_concurrency_limit)

        # 2. Table Processing
        vlm_table_processing = TableProcessing(
            vlm_processor=vlm_processor,
            logger=logger
        )
        steps.append(vlm_table_processing)
    
    if 'content_structuring' in args.steps:
        logger.info(f"Process Content Structuring")

        content_structurer = ContentStructurer(logger=logger)
        document_provider = DocumentProvider(logger=logger)

        # 3. Content Structuring
        content_structuring = ContentStructuring(
            document_provider=document_provider,
            content_structurer=content_structurer,
            logger=logger
        )
        steps.append(content_structuring)

    if 'db_loading' in args.steps:
        logger.info(f"Process DB Loading")

        db_config = {
            'host': '10.20.49.50',
            'port': 13306,
            'user': 'kindapp',
            'password': 'Claimmng!@34',
            'dbname': 'dbkind'
        }

        db_uploader = DBUploader(db_config=db_config, logger=logger)
        db_uploading = DBUploading(uploader=db_uploader,logger=logger)
        steps.append(db_uploading)
    
    initial_context = {
        "output_dir": Path(args.output_dir),
        "file_list_path": Path(args.file_list_path),
    }

    orchestrator = PipelineOrchestrator(steps=steps)
    
    orchestrator.run(initial_context)

    logger.info("Processing pipeline finished successfully.")

if __name__ == "__main__":
    main()