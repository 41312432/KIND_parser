from dataclasses import dataclass, field, fields
from typing import List, Optional, Dict, Any
import re

@dataclass
class FileInfo:
    type: str
    title: str
    fileName: str
    parentFileName: Optional[str] = None
    topParentFileName: Optional[str] = None
    
    _extra: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """딕셔너리에서 FileInfo 객체를 안전하게 생성하는 팩토리 메소드."""
        known_fields = {f.name for f in fields(cls)}
        
        init_args = {k: v for k, v in data.items() if k in known_fields}
        
        extra_args = {k: v for k, v in data.items() if k not in known_fields}
        
        instance = cls(**init_args)
        instance._extra = extra_args
        return instance

    def to_dict(self) -> Dict[str, Any]:
        """FileInfo 객체를 JSON 직렬화를 위한 딕셔너리로 변환합니다."""
        data = self._extra.copy()
        for f in fields(self):
            if f.name != '_extra':
                value = getattr(self, f.name)
                if value is not None:
                    data[f.name] = value
        return data

@dataclass
class MetaInfo:
    id: str
    code: str
    name: str
    attachedInfos: Optional[List[Dict[str, Any]]] = None
    lawInfos: Optional[List[Dict[str, Any]]] = None
    fileInfos: Optional[List[FileInfo]] = None
    
    _extra: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """딕셔너리에서 MetaInfo 객체를 안전하게 생성하는 팩토리 메소드."""
        known_fields = {f.name for f in fields(cls)}

        init_args = {k: v for k, v in data.items() if k in known_fields}
        extra_args = {k: v for k, v in data.items() if k not in known_fields}
        
        # fileInfos가 있다면, 내부의 딕셔너리들을 FileInfo.from_dict를 사용해 객체로 변환
        if 'fileInfos' in init_args and init_args['fileInfos']:
            init_args['fileInfos'] = [FileInfo.from_dict(fi) for fi in init_args['fileInfos']]

        instance = cls(**init_args)
        instance._extra = extra_args
        return instance

    def to_dict(self) -> Dict[str, Any]:
        """MetaInfo 객체를 JSON 직렬화를 위한 딕셔너리로 변환합니다."""
        data = self._extra.copy()
        for f in fields(self):
            if f.name != '_extra':
                value = getattr(self, f.name)
                if value is not None:
                    if f.name == 'fileInfos' and value:
                        data[f.name] = [fi.to_dict() for fi in value]
                    else:
                        data[f.name] = value
        return data
        
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