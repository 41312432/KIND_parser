import logging
from typing import List, Dict, Any
from core.processing import ProcessingStep

logger = logging.getLogger(__name__)

class PipelineOrchestrator:

    def __init__(self, steps: List[ProcessingStep]):
        self._steps = steps

    def run(self, initial_context: Dict[str, Any]) -> None:
        context = initial_context.copy()
        logger.info(f"Orchestrator starting with {len(self._steps)} steps.")
        
        for step in self._steps:
            step_name = step.__class__.__name__
            try:
                logger.info(f"--- Executing step: {step_name} ---")
                step.execute(context)
                logger.info(f"--- Step {step_name} completed successfully. ---")
            except Exception as e:
                logger.error(f"!!! Critical error in step {step_name}: {e} !!!", exc_info=True)
                return