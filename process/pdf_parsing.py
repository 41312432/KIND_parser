import logging
from typing import List, Dict, Any
from pathlib import Path

from core.processing import ProcessingStep
from process.base_step import BaseStep
from service_object.document_provider import DocumentProvider
from service_object.pdf_converter import PDFConverter
from models.mcp_infos import FileInfo, DocumentTree
from utils.utils import create_directory, FolderName

class PDFParsing(BaseStep, ProcessingStep):

    def __init__(self, document_provider: DocumentProvider, pdf_converter: PDFConverter, 
                 logger: logging.Logger):
        super().__init__(logger)
        self.document_provider = document_provider
        self.pdf_converter = pdf_converter

    def execute(self, context: Dict[str, Any]) -> None:
        output_dir: Path = context["output_dir"]
        file_list_path: Path = context["file_list_path"]
        
        create_directory(output_dir)
        
        source_dirs = self._get_file_list(file_list_path)
        context["source_dirs"] = source_dirs 

        for source_dir in sorted(source_dirs):
            meta_info = self.document_provider.get_meta_info(source_dir)
            if not meta_info or not meta_info.fileInfos:
                self.logger.warning(f"Skipping directory due to missing meta or fileInfos: {source_dir}")
                continue

            self.logger.info(f"Processing PDF files for: {meta_info.id}_{meta_info.name}")
            result_path = output_dir / meta_info.id
            doc_tree = self.document_provider.get_document_tree(meta_info)

            for root_node in doc_tree.get_root_nodes():
                self._process_node_recursively(root_node, source_dir, result_path, doc_tree)

            self._process_special_file_list(
                file_list = meta_info.attachedInfos,
                base_path=source_dir,
                result_path=result_path,
                folder_name=FolderName.ATTACHMENT
            )

            # self._process_special_file_list(
            #     file_list = meta_info.lawInfos,
            #     base_path=source_dir,
            #     result_path=result_path,
            #     folder_name=FolderName.LAW
            # )
            # !NOTE: WE DONT NEED LAWS

    def _process_node_recursively(self, node: FileInfo, base_path: Path, current_output_path: Path, doc_tree: DocumentTree):
        node_path = current_output_path / self.document_provider.sanitize_title(node.title)
        create_directory(node_path)
        
        pdf_file_path = self.document_provider.get_pdf_path(node, base_path)
        
        if pdf_file_path:
            self.logger.info(f"Converting: {pdf_file_path}")
            self.pdf_converter.convert_file(file_path=pdf_file_path, result_dir=node_path)

        for child_node in doc_tree.get_children(node):
            if child_node.type not in ['gwan', 'jo']:
                self._process_node_recursively(child_node, base_path, node_path, doc_tree)

    def _process_special_file_list(self, file_list: List[Dict], base_path: Path, result_path: Path, folder_name: str):
        # for attached, law

        if not file_list: return
        
        self.logger.info(f"Processing PDF files for attachement '{folder_name}")

        target_path = result_path / folder_name
        create_directory(target_path)

        for file_info in file_list:
            title = file_info.get('title')
            file_name = file_info.get('fileName')

            sanitized_title = self.document_provider.sanitize_title(title)
            item_result_dir = target_path / sanitized_title
            create_directory(item_result_dir)

            pdf_path = base_path / file_name
            if pdf_path.exists():
                self.pdf_converter.convert_file(pdf_path, item_result_dir)
            else:
                self.logger.warning(f"Attached PDF not found, skipping: {pdf_path}")