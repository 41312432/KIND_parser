import re

ARTICLE_PATTERN = re.compile(r'^##\s*제\s*(\d+)\s*조\s*\((?:[^()]|\([^()]*\))*\)$')
SECTION_PATTERN = re.compile(r'^##\s*(.+)$')