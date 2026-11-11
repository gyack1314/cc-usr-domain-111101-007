"""外部输入分区来源与计划快照。

计划阶段读取并固定每个被引用分区的内容摘要；运行阶段只允许通过快照读取，
分区内容一旦与计划摘要不符即判计划过期，不会悄悄读到新文件。
目录中新增未被引用的分区不会出现在快照里，因此不会使节点失效。
"""
from pathlib import Path

from ..records import load_rows
from ..storage import fingerprint
from .errors import PlanStaleError


class StaticSource:
    """内存分区源，用于演示与测试。partitions 为 {id: rows}。"""

    def __init__(self, partitions):
        self._partitions = dict(partitions)

    def partition_ids(self):
        return set(self._partitions)

    def read_partition(self, partition_id):
        if partition_id not in self._partitions:
            raise KeyError('分区不存在：' + partition_id)
        return [dict(row) for row in self._partitions[partition_id]]

    def partition_digest(self, partition_id):
        return fingerprint(self.read_partition(partition_id))


class DirectorySource:
    """目录分区源：partitions.json 风格清单或 {id: path} 映射。

    分区 id 到文件的映射是显式的；目录里新增的其它文件（无关分区）不参与任何摘要。
    """

    def __init__(self, root, manifest=None):
        self.root = Path(root)
        if manifest is None:
            manifest = self.root / 'partitions.json'
        entries = manifest if isinstance(manifest, dict) else _load_manifest(manifest)
        self._files = {}
        for partition_id, relative in entries.items():
            target = (self.root / relative).resolve()
            if not target.is_relative_to(self.root.resolve()):
                raise ValueError('分区路径超出资料目录：' + partition_id)
            self._files[partition_id] = target

    def partition_ids(self):
        return set(self._files)

    def read_partition(self, partition_id):
        if partition_id not in self._files:
            raise KeyError('分区不存在：' + partition_id)
        return load_rows(self._files[partition_id])

    def partition_digest(self, partition_id):
        return fingerprint(self.read_partition(partition_id))


def _load_manifest(path):
    from ..storage import read_json
    raw = read_json(path)
    if isinstance(raw, dict) and 'partitions' in raw:
        raw = raw['partitions']
    if not isinstance(raw, list) or not all(isinstance(item, dict) and 'id' in item for item in raw):
        raise ValueError('分区清单必须是含 id 的对象数组')
    base = Path(path).parent
    return {item['id']: base / item.get('path', item['id']) for item in raw}


class SnapshotSource:
    """计划快照：构造时固定 {分区id: 摘要}，读取时重新摘要并比对。"""

    def __init__(self, source, digest_map, graph_digest):
        self._source = source
        self._digest_map = dict(digest_map)
        self.graph_digest = graph_digest

    def partition_ids(self):
        return set(self._digest_map)

    def read_partition(self, partition_id):
        if partition_id not in self._digest_map:
            raise PlanStaleError('计划未声明分区：' + partition_id)
        try:
            rows = self._source.read_partition(partition_id)
        except KeyError:
            raise PlanStaleError('计划引用的分区已消失：' + partition_id) from None
        actual = fingerprint(rows)
        if actual != self._digest_map[partition_id]:
            raise PlanStaleError('分区内容已变化，计划过期：' + partition_id)
        return rows

    def verify(self, current_graph_digest):
        """运行前一次性核验：图版本与全部输入分区摘要。"""
        if current_graph_digest != self.graph_digest:
            raise PlanStaleError('图版本已变化，计划过期')
        for partition_id, expected in self._digest_map.items():
            try:
                actual = self._source.partition_digest(partition_id)
            except KeyError:
                raise PlanStaleError('计划引用的分区已消失：' + partition_id) from None
            if actual != expected:
                raise PlanStaleError('分区内容已变化，计划过期：' + partition_id)
