from pathlib import Path
from typing import List

def create_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def read_toc_2_list(toc_path: Path) -> List[str]:
    with open(toc_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f.readlines() if line.strip()]

def read_content(content_path: Path) -> str:
    with open(content_path, 'r', encoding='utf-8') as f:
        return f.read()