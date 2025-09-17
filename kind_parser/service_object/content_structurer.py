import os
import re
import logging
from pathlib import Path
from typing import Dict, List

from utils.utils import read_toc_2_list, read_content
from utils.patterns import ARTICLE_PATTERN
from utils.constants import FileName, FolderName

class ContentStructurer:
    def __init__(self, logger: logging.Logger):
        self.article_pattern = ARTICLE_PATTERN
        self.logger = logger

    def _get_content_path(self, directory: Path) -> Path | None:
        content3_path = directory / 'content3.md'
        content2_path = directory / 'content2.md'
        content_path = directory / FileName.CONTENT

        if content3_path.exists():
            return content3_path
        if content2_path.exists():
            return content2_path
        if content_path.exists():
            return content_path
        return None

    def _split_content_by_sections(self, content: str, sections: List[str]) -> Dict[str, str]:
        result = {}
        section_pattern_regex = re.compile(r'^##\s*(제\d+관.*)', re.MULTILINE)
        matches = list(section_pattern_regex.finditer(content))
        
        if not matches:
            if sections:
                result[sections[0]] = content
            return result

        for i, match in enumerate(matches):
            section_title_from_content = match.group(1).strip()
            start_index = match.start()
            end_index = matches[i + 1].start() if i + 1 < len(matches) else len(content)
            
            section_content = content[start_index:end_index]
            
            found_title_in_toc = next((toc_title for toc_title in sections if section_title_from_content in toc_title or toc_title in section_title_from_content), section_title_from_content)
            result[found_title_in_toc] = section_content.strip()
            
        return result

    def _split_content_by_articles(self, content: str) -> Dict[str, str]:
        result = {}
        current_article = None
        current_content = []
        
        for line in content.split('\n'):
            match = self.article_pattern.match(line.strip())
            if match:
                if current_article:
                    result[current_article] = '\n'.join(current_content).strip()
                current_article = line.strip()
                current_content = [line]
            elif current_article:
                current_content.append(line)
        
        if current_article:
            result[current_article] = '\n'.join(current_content).strip()
        
        return result

    def structure_by_gwan(self, directory: Path) -> None:
        dirs_to_process = []
        for root, dirs, files in os.walk(directory):
            root_path = Path(root)
            if (root_path / FileName.TOC).exists() and self._get_content_path(root_path):
                dirs_to_process.append(root_path)

        for source_dir in dirs_to_process:
            self.logger.info(f"Processing sections for Gwan in: {source_dir}")
            try:
                content_path = self._get_content_path(source_dir)
                content = read_content(content_path)
                sections = read_toc_2_list(source_dir / FileName.TOC)
                split_contents = self._split_content_by_sections(content, sections)

                if len(split_contents) > 1:
                    for section_title, section_content in split_contents.items():
                        sanitized_title = re.sub(r'[\\/*?:"<>|]', '_', section_title).strip()
                        section_dir = source_dir / sanitized_title
                        section_dir.mkdir(exist_ok=True)
                        with open(section_dir / content_path.name, 'w', encoding='utf-8') as f:
                            f.write(section_content)
                        self.logger.info(f"Created subsection: {section_dir}")
            except Exception as e:
                self.logger.error(f"Failed during section splitting for {source_dir}: {e}", exc_info=True)

    def structure_by_jo(self, directory: Path) -> None:
        for root, dirs, files in os.walk(directory):
            root_path = Path(root)
            if not self._should_process_directory_for_jo(root_path):
                continue
            
            content_path = self._get_content_path(root_path)
            if content_path:
                self.logger.info(f"Processing articles for Jo in: {root_path}")
                content = read_content(content_path)
                split_contents = self._split_content_by_articles(content)
                
                if split_contents:
                    for title, article_content in split_contents.items():
                        sanitized_name = re.sub(r'[\\/*?:"<>|]', '_', title.replace('## ', '').strip()).strip()
                        article_dir = root_path / sanitized_name
                        article_dir.mkdir(exist_ok=True)
                        with open(article_dir / content_path.name, 'w', encoding='utf-8') as f:
                            f.write(article_content)
                        self.logger.info(f"Created article: {article_dir / content_path.name}")

    def _should_process_directory_for_jo(self, path: Path) -> bool:
        if FolderName.LAW in str(path) or FolderName.ATTACHMENT in str(path):
            return False
        if (path / FileName.TOC).exists():
            has_gwan_subfolder = any(d.name.startswith("제") and "관" in d.name for d in path.iterdir() if d.is_dir())
            if has_gwan_subfolder:
                return False
        return True