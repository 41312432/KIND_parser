import logging
import os
from typing import List, Dict, Any
from pathlib import Path

from multiprocessing import Pool, cpu_count

from core.processing import ProcessingStep
from process.base_step import BaseStep
from service_object.document_provider import DocumentProvider
from service_object.pdf_converter import PDFConverter
from models.mcp_infos import FileInfo, DocumentTree
from utils.utils import create_directory
from utils.constants import FolderName
import ops_logging


def process_document(task: Dict[str, Any]):
    
    source_dir = task["source_dir"]
    output_dir = task["output_dir"]

    pid = os.getpid()
    worker_logger = ops_logging.get_logger(f"worker-{pid}")
    # worker_logger = logging.getLogger(f"worker_{pid}")
    # if not worker_logger.handlers:
    #     handler = logging.StreamHandler()
    #     formatter = logging.Formatter(f'[%(asctime)s][Worker-{pid}][{source_dir.name}] %(message)s')
    #     handler.setFormatter(formatter)
    #     worker_logger.addHandler(handler)
    #     worker_logger.setLevel(logging.INFO)

    doc_provider = DocumentProvider(logger=worker_logger)
    converter = PDFConverter(
        logger=worker_logger,
        model_path=task["model_path"],
        num_threads=task["num_threads"],
        image_resolution=task["image_resolution"],
    )

    meta_info = doc_provider.get_meta_info(source_dir)

    if not meta_info:
        worker_logger.warning(f"Skipping {source_dir} due to missing meta info")
        return
    
    worker_logger.info(f"Processing started for {meta_info.id}_{meta_info.name}")
    result_path = output_dir / meta_info.id

    def _process_node_recursively_worker(node, base_path, current_output_path, doc_tree, doc_provider, converter, logger):
        node_path = current_output_path / doc_provider.sanitize_title(node.title)
        create_directory(node_path)
        pdf_file_path = doc_provider.get_pdf_path(node, base_path)
        if pdf_file_path:
            logger.info(f"Converting main doc: {pdf_file_path}")
            converter.convert_file(file_path=pdf_file_path, result_dir=node_path)
        for child_node in doc_tree.get_children(node):
            if child_node.type not in ['gwan', 'jo']:
                _process_node_recursively_worker(child_node, base_path, node_path, doc_tree, doc_provider, converter, logger)

    def _process_special_file_list_worker(file_list, base_path, result_path, folder_name, doc_provider, converter, logger):
        if not file_list: return
        target_path = result_path / folder_name
        create_directory(target_path)
        for file_info in file_list:
            title = file_info.get('title')
            file_name = file_info.get('fileName')
            if not title or not file_name: continue
            sanitized_title = doc_provider.sanitize_title(title)
            item_result_dir = target_path / sanitized_title
            create_directory(item_result_dir)
            pdf_path = base_path / file_name
            if pdf_path.exists():
                logger.info(f"Converting attachment: {pdf_path}")
                converter.convert_file(pdf_path, item_result_dir)
            else:
                logger.warning(f"Attached PDF not found: {pdf_path}")

    if meta_info.fileInfos:
        doc_tree = doc_provider.get_document_tree(meta_info)
        for root_node in doc_tree.get_root_nodes():
            _process_node_recursively_worker(
                node=root_node, 
                base_path=source_dir, 
                current_output_path=result_path, 
                doc_tree=doc_tree, 
                doc_provider=doc_provider,
                converter=converter,
                logger=worker_logger
            )
        _process_special_file_list_worker(
            file_list=meta_info.attachedInfos, 
            base_path=source_dir, 
            result_path=result_path, 
            folder_name=FolderName.ATTACHMENT,
            doc_provider=doc_provider,
            converter=converter,
            logger=worker_logger
            )

class PDFParsing(BaseStep, ProcessingStep):

    def __init__(self, document_provider: DocumentProvider, pdf_converter_config: Dict[str, Any], 
                 logger: logging.Logger, num_workers: int):
        super().__init__(logger)
        self.document_provider = document_provider
        self.pdf_converter_config = pdf_converter_config
        self.num_workers = num_workers

    def execute(self, context: Dict[str, Any]) -> None:
        output_dir: Path = context["output_dir"]
        file_list_path: Path = context["file_list_path"]
        
        create_directory(output_dir)
        
        source_dirs = self._get_file_list(file_list_path)
        context["source_dirs"] = source_dirs 

        tasks = []

        for source_dir in source_dirs:
            task = {
                "source_dir": source_dir,
                "output_dir": output_dir,
                "model_path": self.pdf_converter_config["model_path"],
                "num_threads": self.pdf_converter_config["num_threads"],
                "image_resolution": self.pdf_converter_config["image_resolution"],
                "timeout": 120
            }
            tasks.append(task)

        self.logger.info(f"Starting PDF conversion for {len(tasks)} documents using {self.num_workers} workers")

        with Pool(processes=self.num_workers) as pool:
            pool.map(process_document, tasks)

        self.logger.info("All PDF conversion tasks completed")