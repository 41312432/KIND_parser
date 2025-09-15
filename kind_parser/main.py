import sys
import os
import logging
from pathlib import Path
import ops_logging

from multiprocessing import Process

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.arg_parser import get_args
from utils.db import execute, get_status_target_list_query
from core.orchestrator import PipelineOrchestrator

from service_object.pdf_converter import PDFConverter
from service_object.document_provider import DocumentProvider
from service_object.vlm_table_processor import VLMTableProcessor
from service_object.content_structurer import ContentStructurer
from service_object.db_uploader import DBUploader

from process.pdf_parsing import PDFParsing
from process.table_processing import TableProcessing
from process.content_structuring import ContentStructuring
from process.db_uploading import DBUploading, config

import pymysql

logger = ops_logging.get_logger("service")


def run_local_worker(task_info: dict):
    pid = os.getpid()
    worker_logger = ops_logging.get_logger(f"worker-{pid}")

    local_gpu_id = task_info["local_gpu_id"]
    global_rank = task_info["global_rank"]
    
    full_target_list = task_info["target_list"]
    world_size = task_info["num_global_worker"]
    
    my_target_list = [
        target for i, target in enumerate(full_target_list)
        if i % world_size == global_rank
    ]
    
    args = task_info["args"]
    pdf_converter_config = {
        "model_path": args.model_path,
        "num_threads": args.accelerator_thread,
        "image_resolution": args.image_resolution,
        "gpu_id": local_gpu_id
    }
    document_provider = DocumentProvider(logger=worker_logger)
    
    pdf_conversion_step = PDFParsing(
        document_provider=document_provider,
        pdf_converter_config=pdf_converter_config,
        logger=worker_logger,
        num_workers=args.pdf_parsing_num_workers,
    )
    
    context = {
        "data_dir": Path(args.data_dir),
        "output_dir": Path(args.output_dir),
        "target_list": my_target_list, # 필터링된 목록을 context에 전달
    }
    
    pdf_conversion_step.execute(context)

def main():
    logger.info("Starting processing pipeline...")
    args = get_args()

    steps = []

    target_list = execute(get_status_target_list_query())

    if 'pdf_conversion' in args.steps:
        logger.info(f"Process PDF Converting")

        pdf_converter_config = {
            "model_path": args.model_path,
            "num_threads": args.accelerator_thread,
            "image_resolution": args.image_resolution
        }

        gpu_worker_tasks = []

        for i in range(args.num_local_worker):
            local_gpu_id = i
            global_rank = args.root_global_worker_id+i

            task_info = {
                "local_gpu_id": local_gpu_id,
                "global_rank": global_rank,
                "num_global_worker": args.num_global_worker,
                "target_list": target_list,
                "args": args # 나머지 모든 인자를 그대로 전달
            }
            gpu_worker_tasks.append(task_info)

        processes = []
        for task in gpu_worker_tasks:
            p = Process(target=run_local_worker, args=(task, ))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

        # with Pool(processes=args.num_local_worker) as pool:
        #     pool.map(run_local_worker, gpu_worker_tasks)

        # document_provider = DocumentProvider(logger=logger)

        # # 1. PDF Parsing
        # pdf_conversion = PDFParsing(
        #     document_provider=document_provider,
        #     pdf_converter_config=pdf_converter_config,
        #     logger=logger,
        #     num_workers=args.pdf_parsing_num_workers
        # )

        # steps.append(pdf_conversion)

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

    logger.info("Processing pipeline finished successfully.")

if __name__ == "__main__":
    main()