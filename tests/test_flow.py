import tempfile
import unittest
from pathlib import Path

from datapack.domain import REGISTRY, execute_transform
from datapack.flow.cache import ContentCache
from datapack.flow.errors import PlanStaleError, PublishRejected
from datapack.flow.executor import Executor
from datapack.flow.ledger import Ledger
from datapack.flow.planner import Planner
from datapack.flow.publisher import Publisher
from datapack.flow.sources import SnapshotSource, StaticSource
from datapack.storage import Conflict, RevisionStore

NODES = [
    {'id': 'east_only', 'transform': 'identity', 'inputs': ['east/2026-11-01'], 'params': {}},
    {'id': 'all_summary', 'transform': 'sum_by',
     'inputs': ['east_only', 'west/2026-11-01'], 'params': {'key': 'city', 'field': 'distance'}},
    {'id': 'keys', 'transform': 'project', 'inputs': ['all_summary'], 'params': {'fields': ['city']}},
]
TARGETS = [{'id': 'daily', 'nodes': ['all_summary', 'keys']}]

PARTITIONS = {
    'east/2026-11-01': [{'city': 'east', 'distance': 10}, {'city': 'east', 'distance': 20}],
    'west/2026-11-01': [{'city': 'west', 'distance': 40}],
    'east/2026-11-02': [{'city': 'east', 'distance': 15}],
}


class Harness:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.source = StaticSource(PARTITIONS)
        self.cache = ContentCache(root / 'cache')
        self.planner = Planner(self.source, self.cache, root / 'plans', REGISTRY)
        self.ledger = Ledger(root / 'ledger.sqlite3')
        self.revisions = RevisionStore(root / 'revisions.sqlite3')
        self.calls = []

        def counting_executor(name, rows, params):
            self.calls.append(name)
            return execute_transform(name, rows, params)

        self.executor_factory = lambda graph: Executor(graph, self.cache, self.ledger, counting_executor)

    def graph(self, nodes=None, targets=None):
        return self.planner.build_validated_graph(nodes or NODES, targets or TARGETS)

    def plan(self, nodes=None, targets=None):
        return self.planner.make_plan(nodes or NODES, targets or TARGETS)

    def run(self, plan, graph=None):
        graph = graph or self.graph()
        return self.executor_factory(graph).run(plan, self.planner.open_snapshot(plan))

    def cleanup(self):
        self.tmp.cleanup()


class FlowCase(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def tearDown(self):
        self.h.cleanup()

    def test_first_run_executes_each_node_once(self):
        plan = self.h.plan()
        report = self.h.run(plan)
        self.assertEqual(sorted(report['executed']), ['all_summary', 'east_only', 'keys'])
        self.assertEqual(sorted(self.h.calls), ['identity', 'project', 'sum_by'])
        self.assertEqual(report['failed'], [])
        self.assertEqual(report['blocked'], [])

    def test_irrelevant_new_partition_does_not_invalidate(self):
        plan = self.h.plan()
        self.h.run(plan)
        self.h.source._partitions['east/2026-11-03'] = [{'city': 'east', 'distance': 99}]
        plan2 = self.h.plan()
        diff = self.h.planner.explain(plan2, plan, current_graph=self.h.graph())
        self.assertTrue(all(item['status'] == 'cache_hit' for item in diff['nodes']))
        self.h.calls.clear()
        report = self.h.run(plan2)
        self.assertEqual(report['executed'], [])
        self.assertEqual(sorted(report['reused']), ['all_summary', 'east_only', 'keys'])

    def test_modified_partition_invalidates_downstream_only(self):
        plan = self.h.plan()
        self.h.run(plan)
        self.h.source._partitions['east/2026-11-01'] = [{'city': 'east', 'distance': 11}]
        plan2 = self.h.plan()
        diff = self.h.planner.explain(plan2, plan, current_graph=self.h.graph())
        statuses = {item['node']: item['status'] for item in diff['nodes']}
        self.assertEqual(statuses['east_only'], 'invalidated')
        self.assertEqual(statuses['all_summary'], 'invalidated')
        self.assertEqual(statuses['keys'], 'invalidated')
        self.assertIn('east/2026-11-01', diff['changed_sources'])
        self.h.calls.clear()
        report = self.h.run(plan2)
        self.assertEqual(sorted(report['executed']), ['all_summary', 'east_only', 'keys'])
        self.h.source._partitions['west/2026-11-01'] = [{'city': 'west', 'distance': 41}]
        plan3 = self.h.plan()
        diff3 = self.h.planner.explain(plan3, plan2, current_graph=self.h.graph())
        self.assertEqual({item['node']: item['status'] for item in diff3['nodes']}['east_only'],
                         'cache_hit')
        self.h.calls.clear()
        self.h.run(plan3)
        self.assertNotIn('identity', self.h.calls)
        self.assertIn('sum_by', self.h.calls)

    def test_param_only_change_invalidates_node_and_descendants(self):
        plan = self.h.plan()
        self.h.run(plan)
        nodes = [dict(node) for node in NODES]
        nodes[1] = dict(nodes[1], params={'key': 'city', 'field': 'distance', 'extra': 1})
        plan2 = self.h.plan(nodes=nodes)
        diff = self.h.planner.explain(plan2, plan, current_graph=self.h.graph(nodes=nodes))
        statuses = {item['node']: item['status'] for item in diff['nodes']}
        self.assertEqual(statuses['east_only'], 'cache_hit')
        self.assertEqual(statuses['all_summary'], 'invalidated')
        self.assertTrue(any('参数' in reason for reason in
                            next(item for item in diff['nodes'] if item['node'] == 'all_summary')['reasons']))

    def test_selector_change_forces_recompute_even_if_content_same(self):
        plan = self.h.plan()
        self.h.run(plan)
        nodes = [
            {'id': 'east_only', 'transform': 'identity',
             'inputs': ['east/2026-11-01', 'east/2026-11-02'], 'params': {}},
            NODES[1], NODES[2],
        ]
        plan2 = self.h.plan(nodes=nodes)
        diff = self.h.planner.explain(plan2, plan, current_graph=self.h.graph(nodes=nodes))
        reasons = next(item for item in diff['nodes'] if item['node'] == 'east_only')['reasons']
        self.assertTrue(any('选择器' in reason for reason in reasons))

    def test_corrupted_cache_is_detected_and_rebuilt(self):
        plan = self.h.plan()
        self.h.run(plan)
        identity = plan['identities']['keys']
        path = self.h.cache._path(identity)
        path.write_text('{"broken": true}', encoding='utf-8')
        diff = self.h.planner.explain(plan)
        self.assertEqual(
            next(item for item in diff['nodes'] if item['node'] == 'keys')['status'],
            'cache_corrupted')
        # explain 识别损坏但未隔离时（文件仍在），执行器同样识别并重建
        path.write_text('{"broken": true}', encoding='utf-8')
        self.h.calls.clear()
        report = self.h.run(plan)
        self.assertEqual(report['executed'], ['keys'])
        self.assertEqual(report['rebuilt'], ['keys'])
        self.assertFalse(self.h.cache.is_corrupted(identity))
        self.assertTrue(list((Path(self.h.tmp.name) / 'cache' / 'quarantine').glob('*.json')))

    def test_retry_same_plan_does_not_rerun_complete_nodes(self):
        plan = self.h.plan()
        first = self.h.run(plan)
        self.assertEqual(first['run_no'], 1)
        self.h.calls.clear()
        second = self.h.run(plan)
        self.assertEqual(second['run_no'], 2)
        self.assertEqual(second['executed'], [])
        self.assertEqual(sorted(second['reused']), ['all_summary', 'east_only', 'keys'])
        self.assertEqual(self.h.calls, [])
        states = {item['node']: item for item in self.h.ledger.plan_states(plan['plan_id'])}
        self.assertEqual(states['keys']['state'], 'succeeded')
        self.assertEqual(len(self.h.ledger.attempts(plan['plan_id'], 'keys')), 1)

    def test_failure_blocks_downstream_and_excludes_stale_success(self):
        def flaky(name, rows, params):
            if name == 'sum_by':
                raise RuntimeError('模拟中断')
            return execute_transform(name, rows, params)
        graph = self.h.graph()
        plan = self.h.plan()
        report = Executor(graph, self.h.cache, self.h.ledger, flaky).run(
            plan, self.h.planner.open_snapshot(plan))
        self.assertEqual(report['failed'], ['all_summary'])
        self.assertEqual(report['blocked'], ['keys'])
        self.assertNotIn('keys', report['completed'])
        states = {item['node']: item['state']
                  for item in self.h.ledger.plan_states(plan['plan_id'])}
        self.assertEqual(states['all_summary'], 'failed')
        self.assertEqual(states['keys'], 'blocked')
        with self.assertRaises(PublishRejected):
            Publisher(graph, self.h.cache, self.h.ledger, self.h.revisions).publish(plan)

    def test_failed_node_can_run_again_after_fix(self):
        attempts = {'n': 0}

        def flaky(name, rows, params):
            if name == 'sum_by':
                attempts['n'] += 1
                if attempts['n'] == 1:
                    raise RuntimeError('第一次失败')
            return execute_transform(name, rows, params)

        graph = self.h.graph()
        plan = self.h.plan()
        first = Executor(graph, self.h.cache, self.h.ledger, flaky).run(
            plan, self.h.planner.open_snapshot(plan))
        self.assertEqual(first['failed'], ['all_summary'])
        second = Executor(graph, self.h.cache, self.h.ledger, flaky).run(
            plan, self.h.planner.open_snapshot(plan))
        self.assertEqual(sorted(second['executed']), ['all_summary', 'keys'])
        self.assertEqual(second['failed'], [])
        history = self.h.ledger.attempts(plan['plan_id'], 'all_summary')
        self.assertEqual([item['state'] for item in history], ['failed', 'succeeded'])

    def test_restart_resumes_without_rerunning_complete_nodes(self):
        plan = self.h.plan()
        self.h.run(plan)
        fresh_ledger = Ledger(Path(self.h.tmp.name) / 'ledger.sqlite3')
        fresh_cache = ContentCache(Path(self.h.tmp.name) / 'cache')
        self.h.calls.clear()
        report = Executor(self.h.graph(), fresh_cache, fresh_ledger).run(
            plan, self.h.planner.open_snapshot(plan))
        self.assertEqual(report['executed'], [])
        self.assertEqual(self.h.calls, [])

    def test_stale_plan_rejected_when_partition_changes(self):
        plan = self.h.plan()
        self.h.source._partitions['east/2026-11-01'] = [{'city': 'east', 'distance': 1}]
        with self.assertRaises(PlanStaleError):
            self.h.run(plan)
        self.assertEqual(self.h.calls, [])

    def test_stale_plan_rejected_when_graph_changes(self):
        plan = self.h.plan()
        changed = [dict(node, params={} if node['id'] != 'keys' else {'fields': ['city', 'x']})
                   for node in NODES]
        graph2 = self.h.graph(changed)
        self.assertNotEqual(graph2.digest, plan['graph_digest'])
        with self.assertRaises(PlanStaleError):
            self.h.executor_factory(graph2).run(plan, self.h.planner.open_snapshot(plan))

    def test_snapshot_detects_change_between_verification_and_read(self):
        plan = self.h.plan()
        snapshot = self.h.planner.open_snapshot(plan)
        snapshot.verify(self.h.graph().digest)
        self.h.source._partitions['east/2026-11-01'] = [{'city': 'east', 'distance': 1}]
        with self.assertRaises(PlanStaleError):
            snapshot.read_partition('east/2026-11-01')
        with self.assertRaises(PlanStaleError):
            snapshot.read_partition('unknown/partition')

    def test_publish_gate_and_competing_releases(self):
        graph = self.h.graph()
        plan = self.h.plan()
        self.h.run(plan)
        publisher = Publisher(graph, self.h.cache, self.h.ledger, self.h.revisions)
        result = publisher.publish(plan, expected_release_revision=0)
        self.assertEqual(result['release_revision'], 1)
        # 同内容重复发布按修订目录语义幂等，不产生新版本
        self.assertEqual(publisher.publish(plan, expected_release_revision=1)['release_revision'], 1)
        # 用过期预期版本发布不同内容必须落败
        self.h.source._partitions['west/2026-11-01'] = [{'city': 'west', 'distance': 77}]
        plan2 = self.h.plan()
        self.h.run(plan2)
        with self.assertRaises(Conflict):
            publisher.publish(plan2, expected_release_revision=0)
        self.assertEqual(publisher.current_release_revision(), 1)
        # 持最新预期版本即可正常发布
        self.assertEqual(publisher.publish(plan2, expected_release_revision=1)['release_revision'], 2)

    def test_competing_plans_loser_keeps_results_without_overwriting(self):
        graph = self.h.graph()
        plan_a = self.h.plan()
        self.h.run(plan_a)
        self.h.source._partitions['west/2026-11-01'] = [{'city': 'west', 'distance': 77}]
        plan_b = self.h.plan()
        self.h.run(plan_b)
        publisher = Publisher(graph, self.h.cache, self.h.ledger, self.h.revisions)
        winner = publisher.publish(plan_b, expected_release_revision=0)
        self.assertEqual(winner['plan_id'], plan_b['plan_id'])
        with self.assertRaises(Conflict):
            publisher.publish(plan_a, expected_release_revision=0)
        current = publisher.read_release()
        self.assertEqual(current['value']['plan_id'], plan_b['plan_id'])
        # 落败计划的产物仍然完整可读
        loser_identity = plan_a['identities']['keys']
        self.assertIsNotNone(self.h.cache.get(loser_identity))
        self.assertEqual(
            self.h.ledger.state(plan_a['plan_id'], 'keys')['state'], 'succeeded')

    def test_old_revisions_remain_readable(self):
        graph = self.h.graph()
        plan1 = self.h.plan()
        self.h.run(plan1)
        publisher = Publisher(graph, self.h.cache, self.h.ledger, self.h.revisions)
        publisher.publish(plan1, 0)
        self.h.source._partitions['west/2026-11-01'] = [{'city': 'west', 'distance': 5}]
        plan2 = self.h.plan()
        self.h.run(plan2)
        publisher.publish(plan2, 1)
        first = publisher.read_release(1)
        second = publisher.read_release(2)
        self.assertEqual(first['value']['plan_id'], plan1['plan_id'])
        self.assertEqual(second['value']['plan_id'], plan2['plan_id'])
        old_rows = first['value']['artifacts']['daily']['all_summary']['rows']
        new_rows = second['value']['artifacts']['daily']['all_summary']['rows']
        self.assertEqual(old_rows, [{'city': 'east', 'distance': 30}, {'city': 'west', 'distance': 40}])
        self.assertEqual(new_rows, [{'city': 'east', 'distance': 30}, {'city': 'west', 'distance': 5}])

    def test_identity_independent_of_graph_version(self):
        plan = self.h.plan()
        self.h.run(plan)
        # 只改与目标无关的图描述（新增一个不相连节点），图版本变化但身份不变
        nodes = NODES + [{'id': 'standalone', 'transform': 'identity',
                          'inputs': ['east/2026-11-02'], 'params': {}}]
        targets = TARGETS + [{'id': 'other', 'nodes': ['standalone']}]
        plan2 = self.h.plan(nodes=nodes, targets=targets)
        for node_id in plan['identities']:
            self.assertEqual(plan['identities'][node_id], plan2['identities'][node_id])


if __name__ == '__main__':
    unittest.main()
