import pymysql
import logging
import json
import re
from typing import Dict, Optional, List
from pathlib import Path
from utils import db #FIXME: Using db.py method

def natural_sort_key(s: str) -> List:
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

class DBUploader:

    def __init__(self, db_config: Dict, logger: logging.Logger):
        self.db_config = db_config
        self.logger = logger
        self.conn = None
        self.cursor = None
        self.BODY_TABLE = 'product_terms_content_body'
        self.ATTACH_TABLE = 'product_terms_content_attach'
        self.meta_info = {}

        self.current_category_idx = 0
        self.current_category_no = 0
        self.current_category_seq = 0
        self.saved_body_data = []
        self.saved_attach_data = []

    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.db_config['host'], port=self.db_config['port'],
                user=self.db_config['user'], password=self.db_config['password'],
                database=self.db_config['dbname'], charset='utf8mb4'
            )
            self.cursor = self.conn.cursor()
        except pymysql.Error as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def initialize_for_product(self, product_folder: Path, target_id: str):
        self._connect()
        self.saved_body_data = []
        self.saved_attach_data = []
        self.current_category_no = 0
        self.current_category_seq = 0
        
        self.cursor.execute(f"SELECT MAX(category_idx) FROM {self.BODY_TABLE}")
        result = self.cursor.fetchone()
        self.current_category_idx = result[0] if result[0] is not None else 0
        
        meta_path = product_folder / 'termsFileList.json'
        if meta_path.exists():
            with open(meta_path, 'r', encoding='utf-8') as f:
                self.meta_info = json.load(f)
        else:
            self.logger.warning(f"Metadata file (termsFileList.json) not found in {product_folder}")
            self.meta_info = {}

        # 기존 데이터 삭제 !TODO: DO WE NEED?
        self.logger.info(f"Deleting existing data for id: {target_id}")
        self.cursor.execute(f"DELETE FROM {self.BODY_TABLE} WHERE id = %s", (target_id,))
        self.cursor.execute(f"DELETE FROM {self.ATTACH_TABLE} WHERE id = %s", (target_id,))
        self.conn.commit()
        self.logger.info("Delete committed.")

    def upload_main_terms(self, base_folder: Path):
        self.logger.info("--- Starting main terms upload ---")
        items_to_process = [d for d in base_folder.iterdir() if d.is_dir() and d.name not in ['별표', '법률', 'page', 'pictures', 'table', 'table_images', 'table_parsed']]
        
        for item in sorted(items_to_process, key=lambda p: natural_sort_key(p.name)):
            self._process_body_folder_recursively(item, 0, None)
    
    def _process_body_folder_recursively(self, folder_path: Path, depth: int, parent_file_id: Optional[str]):
        folder_name = folder_path.name
        
        file_info = self._get_body_file_info(folder_name)
        my_file_id = ''
        if file_info:
            my_file_id = re.sub(r'(_splitSheet)?\.pdf$', '', file_info.get('fileName', ''))
        
        file_id = my_file_id if my_file_id else parent_file_id

        content_md, content_text, is_file_parent = self._read_content_files(folder_path)

        if depth == 0:
            self.current_category_no += 1
            self.current_category_seq = 0
        self.current_category_seq += 1
        
        self._save_body_to_db(
            id=self.meta_info.get('id', ''), file_id=file_id, is_file_parent=is_file_parent,
            category_no=self.current_category_no, category_seq=self.current_category_seq,
            category_layer=depth + 1, category_nm=folder_name,
            content_md=content_md, content_text=content_text
        )
        
        sub_items = [d for d in folder_path.iterdir() if d.is_dir() and d.name not in ['page', 'pictures', 'table', 'table_images', 'table_parsed']]
        for item in sorted(sub_items, key=lambda p: natural_sort_key(p.name)):
            self._process_body_folder_recursively(item, depth + 1, file_id)

    def upload_attachments(self, base_folder: Path):
        self.logger.info("--- Starting attachments/laws upload ---")
        attach_folder = base_folder / "별표" #TODO: FileName
        if attach_folder.is_dir():
            self._process_attach_folder(attach_folder, 'attach')
        
        #!NOTE: Laws 안다룸
        # law_folder = base_folder / "법률"
        # if law_folder.is_dir():
        #     self._process_attach_folder(law_folder, 'law')

    def _process_attach_folder(self, folder_path: Path, content_type: str):
        for item_path in sorted(folder_path.iterdir()):
            if not item_path.is_dir():
                continue

            file_info = self._get_attach_file_info(item_path.name, content_type)
            if not file_info:
                continue

            file_id = re.sub(r'(_splitSheet)?\.pdf$', '', file_info.get('fileName', ''))
            category_nm = file_info.get('title', item_path.name)
            content_md, content_text, _ = self._read_content_files(item_path)

            self._save_attach_to_db(
                id=self.meta_info.get('id', ''), file_id=file_id,
                content_type=content_type, category_nm=category_nm,
                content_md=content_md, content_text=content_text
            )
            
    def _read_content_files(self, directory: Path) -> (str, str, bool):
        content_md, content_text = '', ''
        content3_path = directory / 'content3.md'
        content_path = directory / 'content.md'
        txt_path = directory / 'content.txt'
        
        if content3_path.exists():
            content_md = content3_path.read_text(encoding='utf-8')
        elif content_path.exists():
            content_md = content_path.read_text(encoding='utf-8')
        
        is_file_parent = txt_path.exists()
        if is_file_parent:
            content_text = txt_path.read_text(encoding='utf-8')
        
        return content_md, content_text, is_file_parent

    def _get_body_file_info(self, folder_name: str) -> Optional[Dict]:
        if not self.meta_info or 'fileInfos' not in self.meta_info: return None
        for file_info in self.meta_info['fileInfos']:
            if file_info.get('title') == folder_name:
                return file_info
        return None
        
    def _get_attach_file_info(self, folder_name: str, content_type: str) -> Optional[Dict]:
        info_list_key = 'attachedInfos' if content_type == 'attach' else 'lawInfos'
        info_list = self.meta_info.get(info_list_key, [])
        if not info_list: return None
        for file_info in info_list:
            if file_info.get('title', '').strip() == folder_name.strip():
                return file_info
        return None

    def _save_body_to_db(self, **kwargs):
        self.current_category_idx += 1
        kwargs['category_idx'] = self.current_category_idx
        
        columns = ['category_idx', 'id', 'file_id', 'is_file_parent', 'category_no', 
                   'category_seq', 'category_layer', 'category_nm', 'content_md', 'content_text']
        values = [kwargs.get(col) for col in columns]
        
        query = f"INSERT INTO {self.BODY_TABLE} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        try:
            self.cursor.execute(query, values)
            self.saved_body_data.append(kwargs)
        except Exception as e:
            self.logger.error(f"DB INSERT ERROR on '{kwargs.get('category_nm')}': {e}")
            self.conn.rollback()

    def _save_attach_to_db(self, **kwargs):
        columns = ['id', 'file_id', 'content_type', 'category_nm', 'content_md', 'content_text']
        values = [kwargs.get(col) for col in columns]
        
        query = f"INSERT INTO {self.ATTACH_TABLE} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        try:
            self.cursor.execute(query, values)
            self.saved_attach_data.append(kwargs)
        except Exception as e:
            self.logger.error(f"DB INSERT ERROR on '{kwargs.get('category_nm')}': {e}")
            self.conn.rollback()

    def print_summary(self):
        self.logger.info("\n=== DB Save Summary ===")
        self.logger.info(f"Total {len(self.saved_body_data)} items inserted into '{self.BODY_TABLE}'.")
        self.logger.info(f"Total {len(self.saved_attach_data)} items inserted into '{self.ATTACH_TABLE}'.")

    def commit_and_close(self):
        if self.conn:
            try:
                self.conn.commit()
                self.logger.info("All data successfully committed.")
            finally:
                self.cursor.close()
                self.conn.close()
                self.logger.info("Database connection closed.")