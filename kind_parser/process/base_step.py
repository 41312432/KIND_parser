import logging
from pathlib import Path
from typing import List

class BaseStep:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    # def _get_file_list(self, file_path: Path) -> List[Path]:
    #     if not file_path.exists():
    #         self.logger.error(f"File list not found at: {file_path}")
    #         return []
    #     with open(file_path, 'r', encoding='utf-8') as f:
    #         return [Path(line.strip()) for line in f if line.strip()]

    # def _get_file_list(self, target_list: List) -> List[Tuple]:

    #     if not file_path.exists():
    #         self.logger.error(f"File list not found at: {file_path}")
    #         return []
    #     with open(file_path, 'r', encoding='utf-8') as f:
    #         return [Path(line.strip()) for line in f if line.strip()]