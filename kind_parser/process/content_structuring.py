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
        target_list: List = context["target_list"]

        for target in target_list:
            target_id = Path(target['id'])
            result_path = output_dir / target_id
            
            if not result_path.exists():
                self.logger.warning(f"Result directory not found, skipping structuring for: {result_path}")
                continue

            self.logger.info(f"Structuring content for: {result_path.name}")
            self.content_structurer.structure_by_gwan(result_path)
            self.content_structurer.structure_by_jo(result_path)