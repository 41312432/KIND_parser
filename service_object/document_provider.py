import json
import re
import logging
from pathlib import Path
from typing import Optional

from models.mcp_infos import FileInfo, MetaInfo, DocumentTree
from utils.constants import FileName

class DocumentProvider:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def sanitize_title(self, title: str) -> str:
        return re.sub(r'[\\/*?:"<>|]', '_', title).strip()

    def get_meta_info(self, source_dir: Path) -> Optional[MetaInfo]:
        meta_file_path = source_dir / FileName.TERMS_FILE_LIST
        if not meta_file_path.exists():
            self.logger.warning(f"Meta file not found: {meta_file_path}")
            return None
        try:
            with open(meta_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'fileInfos' in data and data['fileInfos']:
                    data['fileInfos'] = [FileInfo(**info) for info in data['fileInfos']]
                return MetaInfo(**data)
        except Exception as e:
            self.logger.error(f"Failed to load or parse meta file {meta_file_path}: {e}")
            return None

    def get_document_tree(self, meta_info: MetaInfo) -> DocumentTree:
        return DocumentTree(meta_info.fileInfos or [])

    def get_pdf_path(self, node: FileInfo, base_path: Path) -> Optional[Path]:
        if node.fileName and ".pdf" in node.fileName:
            pdf_path = base_path / node.fileName
            if pdf_path.exists():
                return pdf_path
            else:
                self.logger.warning(f"PDF file not found at expected path: {pdf_path}")
        return None