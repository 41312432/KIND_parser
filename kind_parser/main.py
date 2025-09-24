import sys
import os
import logging
from pathlib import Path
import ops_logging

from multiprocessing import Process

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.arg_parser import get_args
from utils.db import execute, get_status_target_list_query, config
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

logger = ops_logging.get_logger("service")

def main():
    logger.info("Starting processing pipeline...")
    args = get_args()

    steps = []

    target_list = execute(get_status_target_list_query())

    if 'pdf_conversion' in args.steps:
        logger.info(f"Process 1. Start PDF converting")

        pdf_converter_config = {
            "model_path": args.model_path,
            "accelerator_thread": args.accelerator_thread,
            "image_resolution": args.image_resolution,
            "num_local_worker": args.num_local_worker,
            "num_global_worker": args.num_global_worker,
            "root_global_worker_id": args.root_global_worker_id,
            "pdf_parsing_num_workers": args.pdf_parsing_num_workers
        }

        # 1. PDF Parsing
        pdf_parsing = PDFParsing(pdf_converter_config=pdf_converter_config, logger=logger)

        steps.append(pdf_parsing)

    if 'vlm_processing' in args.steps:
        logger.info(f"Process 2. Table Parsing")

        vlm_processor = VLMTableProcessor(logger=logger, base_url=args.vlm_base_url, model_name=args.vlm_model_name, concurrency_limit=args.vlm_concurrency_limit)

        # 2. Table Processing
        vlm_table_processing = TableProcessing(
            vlm_processor=vlm_processor,
            logger=logger
        )
        steps.append(vlm_table_processing)
    
    if 'content_structuring' in args.steps:
        logger.info(f"Process 3. Content Structuring")

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
        logger.info(f"Process 4. DB Loading")

        db_uploader = DBUploader(db_config=config, logger=logger) #FIXME: Refactoring using db.py
        db_uploading = DBUploading(uploader=db_uploader,logger=logger)
        steps.append(db_uploading)
    
    initial_context = {
        "data_dir": Path(args.data_dir),
        "output_dir": Path(args.output_dir),
        "target_list": target_list,
    }

    orchestrator = PipelineOrchestrator(steps=steps)
    
    orchestrator.run(initial_context)

    logger.info("Processing pipeline finished.")

if __name__ == "__main__":
    main()