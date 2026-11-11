"""内容寻址缓存。

每个缓存对象以节点身份为键，落盘为 JSON（行数据 + 输出摘要 + 变换摘要等元数据）。
读取时重新计算行数据摘要并与记录的输出摘要比对，损坏（截断、篡改、文件丢失）
一律视为未命中并把残文件隔离，由执行器重算。
"""
import time
from pathlib import Path

from ..storage import fingerprint, read_json, save_json
from .errors import CacheCorrupted


class ContentCache:
    def __init__(self, root):
        self.root = Path(root)
        self.objects = self.root / 'objects'
        self.quarantine = self.root / 'quarantine'
        self.objects.mkdir(parents=True, exist_ok=True)
        self.quarantine.mkdir(parents=True, exist_ok=True)

    def _path(self, identity):
        if not isinstance(identity, str) or not all(ch in '0123456789abcdef' for ch in identity):
            raise ValueError('缓存键必须是十六进制摘要')
        return self.objects / (identity + '.json')

    def get(self, identity):
        """命中返回 {'rows': ..., 'output_digest': ...}，损坏/缺失返回 None。"""
        path = self._path(identity)
        if not path.exists():
            return None
        try:
            entry = read_json(path)
            if not isinstance(entry, dict) or set(entry) != {'output_digest', 'transform_digest',
                                                             'params_digest', 'inputs_digest', 'rows'}:
                raise CacheCorrupted('缓存字段不完整')
            if fingerprint(entry['rows']) != entry['output_digest']:
                raise CacheCorrupted('输出摘要不匹配')
            rows = entry['rows']
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise CacheCorrupted('缓存行结构损坏')
        except (OSError, ValueError, KeyError, TypeError, CacheCorrupted):
            self._quarantine(identity, path)
            return None
        return {'rows': rows, 'output_digest': entry['output_digest']}

    def put(self, identity, rows, meta):
        entry = {
            'output_digest': fingerprint(rows),
            'transform_digest': meta['transform_digest'],
            'params_digest': meta['params_digest'],
            'inputs_digest': meta['inputs_digest'],
            'rows': rows,
        }
        save_json(self._path(identity), entry)
        return entry['output_digest']

    def is_corrupted(self, identity):
        """供差异解释使用：存在残文件且无法通过校验。"""
        path = self._path(identity)
        if not path.exists():
            return False
        return self.get(identity) is None

    def _quarantine(self, identity, path):
        try:
            stamp = time.strftime('%Y%m%d%H%M%S')
            target = self.quarantine / (identity + '-' + stamp + '.json')
            suffix = 1
            while target.exists():
                target = self.quarantine / (identity + '-' + stamp + '-' + str(suffix) + '.json')
                suffix += 1
            path.replace(target)
        except OSError:
            pass
