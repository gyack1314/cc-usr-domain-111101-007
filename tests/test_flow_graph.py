import unittest
from datapack.domain import REGISTRY
from datapack.flow.errors import GraphValidationError
from datapack.flow.graph import build_graph

PARTITIONS = {'east/2026-11-01', 'west/2026-11-01'}
NODES = [
    {'id': 'east_only', 'transform': 'identity', 'inputs': ['east/2026-11-01'], 'params': {}},
    {'id': 'all_summary', 'transform': 'sum_by',
     'inputs': ['east_only', 'west/2026-11-01'], 'params': {'key': 'city', 'field': 'distance'}},
    {'id': 'keys', 'transform': 'project', 'inputs': ['all_summary'], 'params': {'fields': ['city']}},
]
TARGETS = [{'id': 'daily', 'nodes': ['all_summary', 'keys']}]


class GraphTests(unittest.TestCase):
    def _build(self, nodes, targets):
        return build_graph(nodes, targets, set(REGISTRY), PARTITIONS)

    def test_valid_graph_has_digest_and_topo_order(self):
        graph = self._build(NODES, TARGETS)
        self.assertEqual(graph.issues, ())
        self.assertEqual(len(graph.digest), 64)
        self.assertEqual(graph.topological_order(), ['east_only', 'all_summary', 'keys'])
        self.assertEqual(graph.closure(['keys']), ['east_only', 'all_summary', 'keys'])
        self.assertEqual(graph.closure(['all_summary']), ['east_only', 'all_summary'])

    def test_missing_edge_and_partition_reported_before_execution(self):
        nodes = [{'id': 'a', 'transform': 'identity', 'inputs': ['ghost'], 'params': {}}]
        targets = [{'id': 't', 'nodes': ['missing_node']}]
        graph = self._build(nodes, targets)
        kinds = {issue.kind for issue in graph.issues}
        self.assertIn('missing_partition', kinds)
        self.assertIn('missing_edge', kinds)
        with self.assertRaises(GraphValidationError) as ctx:
            raise GraphValidationError(graph.issues)
        self.assertEqual(len(ctx.exception.as_dicts()), 2)

    def test_cycle_detected(self):
        nodes = [
            {'id': 'a', 'transform': 'identity', 'inputs': ['b'], 'params': {}},
            {'id': 'b', 'transform': 'identity', 'inputs': ['a'], 'params': {}},
        ]
        graph = self._build(nodes, [])
        self.assertEqual([issue.kind for issue in graph.issues], ['cycle'])

    def test_duplicate_outputs_and_unknown_transform(self):
        nodes = [
            {'id': 'a', 'transform': 'identity', 'inputs': ['east/2026-11-01'], 'params': {}},
            {'id': 'a', 'transform': 'nope', 'inputs': ['east/2026-11-01'], 'params': {}},
            {'id': 'b', 'transform': 'nope', 'inputs': ['a'], 'params': {}},
        ]
        graph = self._build(nodes, [])
        kinds = [issue.kind for issue in graph.issues]
        self.assertIn('duplicate_output', kinds)
        self.assertEqual(kinds.count('unknown_transform'), 2)

    def test_all_issues_collected_together(self):
        nodes = [
            {'id': 'a', 'transform': 'identity', 'inputs': ['b', 'no-partition'], 'params': {}},
            {'id': 'a', 'transform': 'identity', 'inputs': ['a'], 'params': {}},
            {'id': 'b', 'transform': 'identity', 'inputs': 'a', 'params': 'bad'},
        ]
        graph = self._build(nodes, [{'id': 't', 'nodes': ['nobody']}])
        kinds = {issue.kind for issue in graph.issues}
        self.assertEqual(kinds, {'duplicate_output', 'missing_partition',
                                 'missing_edge', 'invalid_declaration'})


if __name__ == '__main__':
    unittest.main()
