import os
import signal
import json
import logging
from contextlib import contextmanager
from pathlib import Path
import pathlib

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    AcceleratorDevice, AcceleratorOptions,
    TableStructureOptions,
    EasyOcrOptions,
)
from docling.datamodel.base_models import InputFormat
from docling_core.types.doc.document import ContentLayer
from docling_core.types.doc import ImageRefMode, TableItem

import pymupdf4llm

class PDFConverter:

    def __init__(self, logger, model_path: str, num_threads: int, image_resolution: float):
        self.logger = logger

        # TODO: Options from args
        self.model_path = model_path
        self.num_threads = num_threads
        self.image_resolution = image_resolution

        self.pipeline_options = None
        self.document_converter = None

        self._setup_pipeline_options()
        self._create_converter(self.pipeline_options)

    def _setup_pipeline_options(self):
        accelerator_options = AcceleratorOptions(num_threads=self.num_threads, device=AcceleratorDevice.CUDA) #By Default

        table_structure_options = TableStructureOptions(mode = 'accurate', do_cell_matching=True) #TODO: Options from ARGs

        ocr_options = EasyOcrOptions(lang=['ko', 'en'])

        self.pipeline_options = PdfPipelineOptions(
            artifacts_path=self.model_path,
            accelerator_options=accelerator_options,
            table_structure_options=table_structure_options,
            do_ocr=True,
            ocr_options=ocr_options,
            generate_page_images=True, #NOTE: By default
            generate_picture_images=True, #NOTE: By default
            images_scale=self.image_resolution
        )

    def _create_converter(self, options: PdfPipelineOptions) -> DocumentConverter:
        self.converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)}
        )

    @contextmanager
    def _timeout_context(self, seconds):
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Time out after {seconds} seconds")
        
        original_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, original_handler)

    def convert_file(self, file_path: Path, result_dir: Path, timeout_seconds=120):
        os.makedirs(result_dir, exist_ok=True)

        text_path = result_dir / 'content.txt'

        md_text = pymupdf4llm.to_markdown(file_path)
        md_text = md_text.replace('**', '')
        pathlib.Path(text_path).write_bytes(md_text.encode())

        try:
            with self._timeout_context(timeout_seconds):
                result = self.converter.convert(str(file_path))
                
                md_path = result_dir / 'content.md'
                with open(md_path, 'w', encoding='utf-8') as f: f.write(result.document.export_to_markdown(included_content_layers={ContentLayer.BODY}, image_mode=ImageRefMode.REFERENCED)) #!NOTE: Furniture for Footer or Header? (Do We Need????)

                meta_path = result_dir / 'meta.json'
                with open(meta_path, 'w', encoding='utf-8') as f: json.dump(result.document.export_to_dict(), f, ensure_ascii=False, indent=4)
                
                table_dir = result_dir / 'table'
                table_dir.mkdir(exist_ok=True)
                for i, table in enumerate(result.document.tables):
                    df = table.export_to_dataframe()
                    df.to_csv(table_dir / f'{i+1}.csv', index=False)
                    df.to_markdown(table_dir / f'{i+1}.md', index=False)

                page_dir = result_dir / 'page'
                page_dir.mkdir(exist_ok=True)
                for page_no, page in result.document.pages.items():
                    page.image.pil_image.save(page_dir / f'{page_no}.png', format="PNG")
                
                table_images_dir = result_dir / 'table_images'
                table_images_dir.mkdir(exist_ok=True)
                table_count = 0
                for element, _ in result.document.iterate_items():
                    if isinstance(element, TableItem):
                        img_path = table_images_dir / f'{table_count}.png'
                        with open(img_path, 'wb') as f:
                            element.get_image(result.document).save(f, 'PNG')
                        table_count += 1
                
                self.logger.info(f"Successfully converted {file_path} and extracted {table_count} table images.")

        except Exception as e:
            self.logger.error(f"Error during conversion of {file_path}: {e}", exc_info=False)