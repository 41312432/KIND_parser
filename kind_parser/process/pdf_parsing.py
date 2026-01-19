import logging
import os
import json
from typing import List, Dict, Any
from pathlib import Path
import shutil
from multiprocessing import Pool, Process

from core.processing import ProcessingStep
from process.base_step import BaseStep
from service_object.document_provider import DocumentProvider
from service_object.pdf_converter import PDFConverter
from utils.utils import create_directory
from utils.constants import FolderName, FileName
import ops_logging

class PDFParsing(BaseStep, ProcessingStep):
    def __init__(self, pdf_converter_config: Dict[str, Any], logger: logging.Logger):
        super().__init__(logger)
        self.pdf_converter_config = pdf_converter_config

    def execute(self, context: Dict[str, Any]) -> None:
        data_dir: Path = context["data_dir"]
        output_dir: Path = context["output_dir"]
        target_list: List[Dict] = context["target_list"]
        create_directory(output_dir)
        gpu_worker_tasks = []
        num_local_workers = self.pdf_converter_config["num_local_worker"]
        for i in range(num_local_workers):
            task = {
                "local_gpu_id": i,
                "global_rank": self.pdf_converter_config["root_global_worker_id"] + i,
                "num_global_worker": self.pdf_converter_config["num_global_worker"],
                "target_list": target_list,
                "data_dir": data_dir,
                "output_dir": output_dir,
                "pdf_converter_config": self.pdf_converter_config,
            }
            gpu_worker_tasks.append(task)
        self.logger.info(f"Distributing PDF conversion tasks to {num_local_workers} GPU workers.")

        processes = []

        for task in gpu_worker_tasks:
            p = Process(target=PDFParsing.run_gpu_worker, args=(task, ))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

        self.logger.info("All PDF conversion tasks have been completed.")

    @staticmethod
    def run_gpu_worker(task_info: Dict[str, Any]):
        pid = os.getpid()
        global_rank = task_info["global_rank"]
        local_gpu_id = task_info['local_gpu_id']
        os.environ['CUDA_VISIBLE_DEVICES'] = str(local_gpu_id)
        worker_logger = ops_logging.get_logger(f"Gpu-Worker-{pid}-GPU-{global_rank}-localGPU-{local_gpu_id}")

        my_target_list = [
            target for i, target in enumerate(task_info["target_list"])
            if i % task_info["num_global_worker"] == global_rank
        ]
        worker_logger.info(f"Worker started. Assigned {len(my_target_list)} documents.")
        
        config = task_info["pdf_converter_config"]
        num_inner_workers = config["pdf_parsing_num_workers"]

        sub_tasks = []
        for target in my_target_list:
            sub_task = {
                "target_info": target,
                "data_dir": task_info["data_dir"],
                "output_dir": task_info["output_dir"],
                "pdf_converter_config": config,
                "local_gpu_id": 0,
                # "local_gpu_id": local_gpu_id, NOTE: It's NCCL Docling BUG (OR P in P ?)
                "parent_pid": pid,
                "parent_gpu_global_rank": global_rank
            }
            sub_tasks.append(sub_task)
            
        try:
            with Pool(processes=num_inner_workers) as inner_pool:
                results = inner_pool.map(PDFParsing.process_document_wrapper, sub_tasks)
                for res in results:
                    worker_logger.info(res)
        except Exception as e:
            worker_logger.critical(f"A critical error occurred in the inner process pool: {e}", exc_info=True)

        worker_logger.info(f"Worker finished processing all assigned documents.")

    @staticmethod
    def process_document_wrapper(task: Dict[str, Any]) -> str:
        worker_pid = os.getpid()
        task['worker_pid'] = worker_pid
        parent_pid = task['parent_pid']
        parent_rank = task['parent_gpu_global_rank']

        target_id = Path(task['target_info']['id'])
        logger = ops_logging.get_logger(worker_pid)
        task['logger'] = logger
        try:
            logger.info(f"[GPU-{parent_rank}][Worker-{parent_pid}][Inner worker-{worker_pid}] Starting job for document {target_id}")
            result = PDFParsing.process_document(task)
            logger.info(f"[GPU-{parent_rank}][Worker-{parent_pid}][Inner worker-{worker_pid}]  Finished job for document {target_id}")
            return result
        except Exception as e:
            logger.error(f"[GPU-{parent_rank}][Worker-{parent_pid}][Inner worker-{worker_pid}] Error processing document {target_id}: {e}", exc_info=True)
            return f"Failed to process document {target_id}"

    @staticmethod
    def process_document(task: Dict[str, Any]) -> str:
        logger = task["logger"]
        target_info = task["target_info"]
        data_dir = task["data_dir"]
        output_dir = task["output_dir"]
        config = task["pdf_converter_config"]
        worker_pid = task["worker_pid"]
        parent_pid = task['parent_pid']
        parent_rank = task['parent_gpu_global_rank']

        converter = PDFConverter(
            logger=logger,
            model_path=config["model_path"],
            num_threads=config["accelerator_thread"],
            image_resolution=config["image_resolution"],
            local_gpu_id=task["local_gpu_id"]
        )
        
        target_id = Path(target_info['id'])
        target_source_dir = data_dir / Path(target_info['pdf_filepath'])

        doc_provider = DocumentProvider(logger=logger)
        meta_info = doc_provider.get_meta_info(target_source_dir)
        
        if not meta_info: return f"[Inner worker-{worker_pid}] Skipped {target_source_dir}: Meta info not found."

        logger.debug(f"[GPU-{parent_rank}][Worker-{parent_pid}][Inner worker-{worker_pid}] Processing started for {meta_info.id}_{meta_info.name}")
        result_path = output_dir / target_id
        create_directory(result_path)
        
        try:
            meta_save_path = result_path / "termsFileList.json"
            meta_dict = meta_info.to_dict()
            with open(meta_save_path, 'w', encoding='utf-8') as f:
                json.dump(meta_dict, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"[GPU-{parent_rank}][Worker-{parent_pid}][Inner worker-{worker_pid}] Failed to copy termsFileList.json for {target_id}: {e}")

        if meta_info.fileInfos:
            doc_tree = doc_provider.get_document_tree(meta_info)
            for root_node in doc_tree.get_root_nodes():
                PDFParsing._process_node_recursively(root_node, target_source_dir, result_path, doc_tree, doc_provider, converter, logger, worker_pid)
            PDFParsing._process_special_files(meta_info.attachedInfos, target_source_dir, result_path, FolderName.ATTACHMENT, doc_provider, converter, logger, worker_pid)
            PDFParsing._process_special_files(meta_info.lawInfos, target_source_dir, result_path, FolderName.LAW, doc_provider, converter, logger, worker_pid)
        
        return f"[GPU-{parent_rank}][Worker-{parent_pid}][Inner worker-{worker_pid}] Successfully processed {meta_info.id}_{meta_info.name}"
    
    @staticmethod
    def _process_node_recursively(node, base_path, current_output_path, doc_tree, doc_provider, converter, logger, worker_pid):
        node_path = current_output_path / doc_provider.sanitize_title(node.title)
        create_directory(node_path)
        children = doc_tree.get_children(node)
        pdf_file_path = doc_provider.get_pdf_path(node, base_path)
        if pdf_file_path:
            gwan_children = [child for child in children if child.type == 'gwan']
            if gwan_children:
                toc_content = "\n".join([child.title for child in gwan_children])
                with open(node_path / FileName.TOC, 'w', encoding='utf-8') as f:
                    f.write(toc_content)
            logger.debug(f"[Worker-{worker_pid}] Converting main doc: {pdf_file_path}")
            converter.convert_file(file_path=pdf_file_path, result_dir=node_path)
        for child_node in children:
            if child_node.type not in ['jo']:
                PDFParsing._process_node_recursively(child_node, base_path, node_path, doc_tree, doc_provider, converter, logger, worker_pid)

    @staticmethod
    def _process_special_files(file_list, base_path, result_path, folder_name, doc_provider, converter, logger, worker_pid):
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
                logger.debug(f"[Worker-{worker_pid}] Converting attachment: {pdf_path}")
                converter.convert_file(pdf_path, item_result_dir)
            else:
                logger.warning(f"[Worker-{worker_pid}] Attached PDF not found: {pdf_path}")