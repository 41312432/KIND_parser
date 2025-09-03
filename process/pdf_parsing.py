import logging
from typing import Dict, Any
from pathlib import Path

from core.processing import ProcessingStep
from process.base_step import BaseStep
from service_object.document_provider import DocumentProvider
from service_object.pdf_converter import PDFConverter
from models.mcp_infos import FileInfo, DocumentTree

class PDFParsing(BaseStep, ProcessingStep):

    def __init__(self, document_provider: DocumentProvider, pdf_converter: PDFConverter, 
                 logger: logging.Logger, image_resolution: float):
        super().__init__(logger)
        self.document_provider = document_provider
        self.pdf_converter = pdf_converter
        self.image_resolution = image_resolution

    def execute(self, context: Dict[str, Any]) -> None:
        output_dir: Path = context["output_dir"]
        file_list_path: Path = context["file_list_path"]
        
        output_dir.mkdir(exist_ok=True, parents=True)
        
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

    def _process_node_recursively(self, node: FileInfo, base_path: Path, current_output_path: Path, doc_tree: DocumentTree):
        node_path = current_output_path / self.document_provider.sanitize_title(node.title)
        node_path.mkdir(exist_ok=True, parents=True)
        
        pdf_file_path = self.document_provider.get_pdf_path(node, base_path)
        
        if pdf_file_path:
            self.logger.info(f"Converting with resolution {self.image_resolution}: {pdf_file_path}")
            self.pdf_converter.convert_file(pdf_file_path, node_path, self.image_resolution)

        for child_node in doc_tree.get_children(node):
            if child_node.type not in ['gwan', 'jo']:
                self._process_node_recursively(child_node, base_path, node_path, doc_tree)