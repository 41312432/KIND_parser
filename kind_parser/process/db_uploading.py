import logging
from typing import Dict, Any, List
from pathlib import Path

from core.processing import ProcessingStep
from process.base_step import BaseStep
from service_object.db_uploader import DBUploader

class DBUploading(BaseStep, ProcessingStep):
    def __init__(self, uploader: DBUploader, logger: logging.Logger):
        super().__init__(logger)
        self.uploader = uploader

    def execute(self, context: Dict[str, Any]) -> None:
        data_dir: Path = context["data_dir"]
        output_dir: Path = context["output_dir"]
        target_list: List = context["target_list"]

        for target in target_list:
            target_id = data_dir / Path(target[0])
            target_folder = output_dir / target_id
            
            self.logger.info(f"\n===== Starting DB upload for: {target_folder} =====")

            if not target_folder.is_dir():
                self.logger.warning(f"Target folder not found, skipping: {target_folder}")
                continue
            
            try:
                # 1. DB 연결, 상태 초기화, 기존 데이터 삭제
                self.uploader.initialize_for_product(target_folder, target_id)

                # 2. 메인 약관 본문 업로드
                self.uploader.upload_main_terms(target_folder)

                # 3. 별표/법률 업로드
                self.uploader.upload_attachments(target_folder)
                
                # 4. 요약 출력
                self.uploader.print_summary()

            except Exception as e:
                self.logger.error(f"!!! Unhandled exception during DB processing for {target_folder}: {e}", exc_info=True)
            finally:
                # 5. 모든 작업 후 커밋 및 연결 종료
                self.uploader.commit_and_close()
        
        self.logger.info("\n===== All DB upload tasks are complete. =====")