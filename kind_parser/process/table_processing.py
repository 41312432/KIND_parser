import logging
import asyncio
from typing import Dict, Any
from pathlib import Path

from core.processing import ProcessingStep
from service_object.vlm_table_processor import VLMTableProcessor

class TableProcessing(ProcessingStep):

    def __init__(self, vlm_processor: VLMTableProcessor, logger: logging.Logger):
        self.vlm_processor = vlm_processor
        self.logger = logger

    def execute(self, context: Dict[str, Any]) -> None:
        output_dir: Path = context["output_dir"]
        
        self.logger.info(f"Starting VLM table processing for directory: {output_dir}")
        asyncio.run(self.vlm_processor.process_all_tables(output_dir))