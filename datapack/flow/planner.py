"""计划器：节点内容身份、计划固定与失效差异解释。

节点身份（缓存键）只覆盖变换代码摘要、规范化参数与实际读取的上游内容引用：
- 外部输入分区用其行内容摘要表示；
- 上游节点用其节点身份表示（Merkle 式传递内容）。
目录中新增未被引用的分区不进入任何身份，因此不会造成失效；
选择器（inputs 列表）本身变化会改变 inputs_digest，必须重算。
图版本另外固定在计划上，用于运行前判断整张图是否被改过。
"""
import time
from pathlib import Path

from ..storage import fingerprint, read_json, save_json
from .cache import ContentCache
from .errors import GraphValidationError
from .graph import build_graph, node_transform_digest
from .sources import SnapshotSource


class Planner:
    def __init__(self, source, cache, plans_dir, registered_transforms):
        self.source = source
        self.cache = cache if isinstance(cache, ContentCache) else ContentCache(cache)
        self.plans_dir = Path(plans_dir)
        self.plans_dir.mkdir(parents=True, exist_ok=True)
        self.registered_transforms = dict(registered_transforms)

    def build_validated_graph(self, raw_nodes, raw_targets):
        graph = build_graph(raw_nodes, raw_targets,
                            set(self.registered_transforms), self.source.partition_ids())
        if graph.issues:
            raise GraphValidationError(graph.issues)
        return graph

    def make_plan(self, raw_nodes, raw_targets, targets=None):
        """固定图版本与输入摘要，计算每个节点的内容身份并持久化计划。"""
        graph = self.build_validated_graph(raw_nodes, raw_targets)
        target_ids = list(graph.targets) if targets is None else list(targets)
        unknown = [target_id for target_id in target_ids if target_id not in graph.targets]
        if unknown:
            raise ValueError('计划引用不存在目标：' + ','.join(unknown))
        wanted_nodes = set()
        for target_id in target_ids:
            wanted_nodes.update(graph.closure(graph.targets[target_id].nodes))

        source_digests = {}
        for node_id in wanted_nodes:
            for reference in graph.nodes[node_id].inputs:
                if reference not in graph.nodes and reference not in source_digests:
                    source_digests[reference] = self.source.partition_digest(reference)

        identities, node_records = self._compute_identities(graph, wanted_nodes, source_digests)
        plan = {
            'plan_id': fingerprint({'graph_digest': graph.digest, 'sources': source_digests,
                                    'targets': target_ids, 'created_at': time.time()}),
            'graph_digest': graph.digest,
            'created_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'targets': target_ids,
            'source_digests': source_digests,
            'nodes': node_records,
            'identities': identities,
        }
        save_json(self.plans_dir / (plan['plan_id'] + '.json'), plan)
        (self.plans_dir / 'latest').write_text(plan['plan_id'], encoding='utf-8')
        return plan

    def _compute_identities(self, graph, wanted_nodes, source_digests):
        identities = {}
        records = {}
        for node_id in graph.topological_order():
            if node_id not in wanted_nodes:
                continue
            node = graph.nodes[node_id]
            input_refs = []
            for reference in node.inputs:
                if reference in identities:
                    input_refs.append({'kind': 'node', 'id': reference, 'digest': identities[reference]})
                else:
                    input_refs.append({'kind': 'partition', 'id': reference,
                                       'digest': source_digests[reference]})
            transform = node_transform_digest(node)
            params_digest = fingerprint(node.params)
            inputs_digest = fingerprint(input_refs)
            identity = fingerprint({
                'node': node_id,
                'transform': node.transform,
                'transform_digest': transform,
                'params_digest': params_digest,
                'inputs_digest': inputs_digest,
            })
            identities[node_id] = identity
            records[node_id] = {
                'id': node_id,
                'transform': node.transform,
                'transform_digest': transform,
                'params_digest': params_digest,
                'inputs_digest': inputs_digest,
                'inputs': input_refs,
                'identity': identity,
            }
        return identities, records

    def open_snapshot(self, plan):
        return SnapshotSource(self.source, plan['source_digests'], plan['graph_digest'])

    def load_plan(self, plan_id=None):
        if plan_id is None:
            pointer = self.plans_dir / 'latest'
            if not pointer.exists():
                raise KeyError('没有已保存的计划')
            plan_id = pointer.read_text(encoding='utf-8').strip()
        return read_json(self.plans_dir / (plan_id + '.json'))

    def explain(self, plan, previous_plan=None, current_graph=None):
        """解释每个节点本次为什么命中/失效，供维护人员先看差异再执行。

        previous_plan：上一次成功尝试对应的计划（默认取最近保存的其它计划）；
        current_graph：当前图声明，用于识别选择器/变换/参数层面的变化。
        """
        if previous_plan is None:
            previous_plan = self._previous_plan(plan['plan_id'])
        prev_nodes = previous_plan['nodes'] if previous_plan else {}
        prev_sources = previous_plan['source_digests'] if previous_plan else {}

        current_nodes = current_graph.nodes if current_graph is not None else {}
        changed_sources = sorted(
            partition_id for partition_id, digest in plan['source_digests'].items()
            if prev_sources.get(partition_id) != digest)

        result = []
        for node_id, record in plan['nodes'].items():
            entry = self._explain_node(node_id, record, prev_nodes.get(node_id),
                                       current_nodes.get(node_id), changed_sources, plan)
            result.append(entry)
        return {
            'plan_id': plan['plan_id'],
            'graph_digest': plan['graph_digest'],
            'changed_sources': changed_sources,
            'nodes': result,
            'summary': {
                'cache_hit': sum(1 for item in result if item['status'] == 'cache_hit'),
                'invalidated': sum(1 for item in result if item['status'] == 'invalidated'),
                'new_node': sum(1 for item in result if item['status'] == 'new_node'),
                'cache_corrupted': sum(1 for item in result if item['status'] == 'cache_corrupted'),
            },
        }

    def _explain_node(self, node_id, record, previous, current_node, changed_sources, plan):
        identity = record['identity']
        path = self.cache._path(identity)
        base = {'node': node_id, 'identity': identity, 'reasons': []}
        if path.exists():
            cached = self.cache.get(identity)
            if cached is not None:
                base['status'] = 'cache_hit'
                base['reasons'].append('身份 ' + identity[:12] + ' 的缓存完整，可直接复用')
                base['output_digest'] = cached['output_digest']
                return base
            base['status'] = 'cache_corrupted'
            base['reasons'].append('缓存内容损坏或被篡改，需要重建')
            return base
        if previous is None:
            base['status'] = 'new_node'
            base['reasons'].append('首次出现的节点，没有历史缓存')
            return base

        reasons = []
        if record['transform_digest'] != previous['transform_digest']:
            reasons.append('变换代码摘要变化（' + record['transform'] + '）')
        if record['params_digest'] != previous['params_digest']:
            if current_node is not None:
                reasons.append('规范化参数变化：' + fingerprint(current_node.params)[:12])
            else:
                reasons.append('规范化参数变化')
        if record['inputs_digest'] != previous['inputs_digest']:
            current_refs = {self._ref_key(ref): ref for ref in record['inputs']}
            previous_refs = {self._ref_key(ref): ref for ref in previous['inputs']}
            added = sorted(key for key in current_refs if key not in previous_refs)
            removed = sorted(key for key in previous_refs if key not in current_refs)
            if added or removed:
                reasons.append('选择器（inputs）本身变化，必须重算'
                               + ('，新增引用 ' + ','.join(added) if added else '')
                               + ('，移除引用 ' + ','.join(removed) if removed else ''))
            upstream = [ref['id'] for ref in record['inputs']
                        if ref['kind'] == 'node'
                        and current_refs.get(self._ref_key(ref), {}).get('digest')
                        != previous_refs.get(self._ref_key(ref), {}).get('digest')]
            if upstream:
                reasons.append('上游节点身份变化：' + ','.join(upstream))
            touched = [ref['id'] for ref in record['inputs']
                       if ref['kind'] == 'partition' and ref['id'] in changed_sources]
            if touched:
                reasons.append('读取的上游分区内容变化：' + ','.join(touched))
            if not reasons:
                reasons.append('上游输入引用摘要变化')
        base['status'] = 'invalidated'
        base['reasons'] = reasons or ['内容身份未命中缓存']
        return base

    @staticmethod
    def _ref_key(reference):
        return reference['kind'] + ':' + reference['id']

    def _previous_plan(self, current_plan_id):
        candidates = sorted(self.plans_dir.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
        for path in candidates:
            if path.stem == current_plan_id:
                continue
            try:
                return read_json(path)
            except ValueError:
                continue
        return None
