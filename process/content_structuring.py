import logging
from typing import Dict, Any, List
from pathlib import Path

from core.processing import ProcessingStep
from process.base_step import BaseStep
from service_object.document_provider import DocumentProvider
from service_object.content_structurer import ContentStructurer

class ContentStructuring(BaseStep, ProcessingStep):

    def __init__(self, document_provider: DocumentProvider, content_structurer: ContentStructurer, logger: logging.Logger):
        super().__init__(logger)
        self.document_provider = document_provider
        self.content_structurer = content_structurer

    def execute(self, context: Dict[str, Any]) -> None:
        output_dir: Path = context["output_dir"]
        source_dirs: List[Path] = context.get("source_dirs", [])
        
        if not source_dirs:
             self.logger.warning("No source directories found in context for structuring.")
             return

        for source_dir in sorted(source_dirs):
            meta_info = self.document_provider.get_meta_info(source_dir)
            if not meta_info:
                continue

            result_path = output_dir / meta_info.id
            if not result_path.exists():
                self.logger.warning(f"Result directory not found, skipping structuring for: {result_path}")
                continue

            self.logger.info(f"Structuring content for: {result_path.name}")
            self.content_structurer.structure_by_gwan(result_path)
            self.content_structurer.structure_by_jo(result_path)