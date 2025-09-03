from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import re

@dataclass
class FileInfo:
    type: str
    title: str
    fileName: str
    parentFileName: Optional[str] = None
    topParentFileName: Optional[str] = None
    _extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        known_fields = {f.name for f in self.__class__.__dataclass_fields__.values()}
        extra_args = {k: v for k, v in self.__dict__.items() if k not in known_fields}
        self._extra = extra_args
        for k in extra_args:
            del self.__dict__[k]

@dataclass
class MetaInfo:
    id: str
    code: str
    name: str
    attachedInfos: Optional[List[Dict[str, Any]]] = None
    lawInfos: Optional[List[Dict[str, Any]]] = None
    fileInfos: Optional[List[FileInfo]] = None
    _extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        known_fields = {f.name for f in self.__class__.__dataclass_fields__.values()}
        extra_args = {k: v for k, v in self.__dict__.items() if k not in known_fields}
        self._extra = extra_args
        for k in extra_args:
            del self.__dict__[k]

class DocumentTree:
    def __init__(self, file_infos: List[FileInfo]):
        self.file_infos = file_infos
        self.parent_child_map = self._build_map()
        self.root_nodes = self._find_roots()

    def _normalize_key(self, file_name: str) -> str:
        return re.sub(r'(_splitSheet)?\.pdf$', '', file_name)

    def _build_map(self) -> Dict[str, List[FileInfo]]:
        parent_child_map = {}
        for f_info in self.file_infos:
            parent_id = f_info.parentFileName
            if parent_id:
                normalized_parent_id = self._normalize_key(parent_id)
                if normalized_parent_id not in parent_child_map:
                    parent_child_map[normalized_parent_id] = []
                parent_child_map[normalized_parent_id].append(f_info)
        return parent_child_map

    def _find_roots(self) -> List[FileInfo]:
        return [
            f_info for f_info in self.file_infos 
            if f_info.type in ["mainSheet", "specialSheet"] and not f_info.parentFileName
        ]

    def get_children(self, node: FileInfo) -> List[FileInfo]:
        if not node.fileName:
            return []
        lookup_key = self._normalize_key(node.fileName)
        return self.parent_child_map.get(lookup_key, [])

    def get_root_nodes(self) -> List[FileInfo]:
        return self.root_nodes