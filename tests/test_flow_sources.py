import json
import tempfile
import unittest
from pathlib import Path

from datapack.domain import REGISTRY
from datapack.flow.cache import ContentCache
from datapack.flow.executor import Executor
from datapack.flow.ledger import Ledger
from datapack.flow.planner import Planner
from datapack.flow.sources import DirectorySource, SnapshotSource, StaticSource
from datapack.flow.errors import PlanStaleError

NODES = [
    {'id': 'east_only', 'transform': 'identity', 'inputs': ['east/day=1'], 'params': {}},
    {'id': 'summary', 'transform': 'sum_by', 'inputs': ['east_only'],
     'params': {'key': 'city', 'field': 'distance'}},
]
TARGETS = [{'id': 't', 'nodes': ['summary']}]


class DirectorySourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / 'east').mkdir()
        (self.root / 'east' / 'day=1.jsonl').write_text(
            '{"city": "east", "distance": 1}\n{"city": "east", "distance": 2}\n',
            encoding='utf-8')
        self.manifest = {'east/day=1': 'east/day=1.jsonl'}

    def tearDown(self):
        self.tmp.cleanup()

    def test_manifest_mapping_and_unrelated_file_ignored(self):
        source = DirectorySource(self.root, self.manifest)
        self.assertEqual(source.partition_ids(), {'east/day=1'})
        rows = source.read_partition('east/day=1')
        self.assertEqual(len(rows), 2)
        # 目录新增无关文件不改变任何分区摘要，也不出现在分区集合里
        unrelated = self.root / 'east' / 'day=2.jsonl'
        unrelated.write_text('{"city": "east", "distance": 99}\n', encoding='utf-8')
        self.assertEqual(source.partition_ids(), {'east/day=1'})
        self.assertEqual(source.partition_digest('east/day=1'),
                         StaticSource({'p': rows}).partition_digest('p'))

    def test_path_escape_rejected(self):
        with self.assertRaises(ValueError):
            DirectorySource(self.root, {'evil': '../../etc/passwd'})

    def test_array_manifest_with_id_and_path(self):
        manifest_path = self.root / 'partitions.json'
        manifest_path.write_text(json.dumps([
            {'id': 'east/day=1', 'path': 'east/day=1.jsonl'}]), encoding='utf-8')
        source = DirectorySource(self.root, manifest_path)
        self.assertEqual(source.partition_ids(), {'east/day=1'})

    def test_end_to_end_with_directory_source(self):
        source = DirectorySource(self.root, self.manifest)
        workdir = self.root / 'state'
        cache = ContentCache(workdir / 'cache')
        planner = Planner(source, cache, workdir / 'plans', REGISTRY)
        ledger = Ledger(workdir / 'ledger.sqlite3')
        plan = planner.make_plan(NODES, TARGETS)
        graph = planner.build_validated_graph(NODES, TARGETS)
        report = Executor(graph, cache, ledger).run(plan, planner.open_snapshot(plan))
        self.assertEqual(sorted(report['executed']), ['east_only', 'summary'])
        # 修改文件后旧计划快照应判过期
        (self.root / 'east' / 'day=1.jsonl').write_text(
            '{"city": "east", "distance": 7}\n', encoding='utf-8')
        with self.assertRaises(PlanStaleError):
            Executor(graph, cache, ledger).run(plan, planner.open_snapshot(plan))

    def test_snapshot_rejects_undeclared_partition(self):
        source = DirectorySource(self.root, self.manifest)
        snapshot = SnapshotSource(source, {}, 'graph-digest')
        with self.assertRaises(PlanStaleError):
            snapshot.read_partition('east/day=1')


if __name__ == '__main__':
    unittest.main()
