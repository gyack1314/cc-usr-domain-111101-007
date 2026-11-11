"""执行器：按计划拓扑执行，命中复用、损坏重建、失败阻断、重启恢复。

执行器接受可注入的 transform_executor（默认调用 domain 注册表），
调用方可通过包装执行器观察真实变换调用次数。
"""
from ..domain import execute_transform
from .errors import PlanStaleError
from .ledger import Ledger


class Executor:
    def __init__(self, graph, cache, ledger, transform_executor=None):
        self.graph = graph
        self.cache = cache
        self.ledger = ledger if isinstance(ledger, Ledger) else Ledger(ledger)
        self.transform_executor = transform_executor or execute_transform

    def run(self, plan, snapshot):
        """执行计划。先核验图版本与全部输入摘要，过期直接抛 PlanStaleError。"""
        snapshot.verify(self.graph.digest)
        plan_id = plan['plan_id']
        run_no = self.ledger.next_run_no(plan_id)
        order = self.graph.closure(
            [node_id for target_id in plan['targets']
             for node_id in self.graph.targets[target_id].nodes])

        reports = []
        failed = set()
        for node_id in order:
            record = plan['nodes'][node_id]
            identity = record['identity']
            node = self.graph.nodes[node_id]

            blocked_by = sorted(reference for reference in node.inputs
                                if reference in failed)
            if blocked_by:
                self.ledger.record(plan_id, node_id, 'blocked', identity, run_no,
                                   error='上游未成功：' + ','.join(blocked_by))
                reports.append({'node': node_id, 'state': 'blocked', 'identity': identity,
                                'blocked_by': blocked_by})
                failed.add(node_id)
                continue

            saved = self.ledger.state(plan_id, node_id)
            entry = self.cache.get(identity)
            if entry is not None:
                # 命中缓存：补齐本计划成功状态（已成功则幂等，不调用变换）
                self.ledger.ensure_succeeded(plan_id, node_id, identity,
                                             entry['output_digest'], run_no)
                reports.append({'node': node_id, 'state': 'reused', 'identity': identity,
                                'output_digest': entry['output_digest'],
                                'source': 'cache'})
                continue
            if saved and saved['state'] == 'succeeded':
                # 台账成功但缓存对象已损坏/缺失：记录不再可信，必须重建
                reports.append({'node': node_id, 'state': 'rebuild', 'identity': identity,
                                'reason': 'ledger_ok_but_cache_missing'})
            elif saved and saved['state'] == 'blocked':
                # 上游已恢复，阻断解除，本节点重新进入执行
                reports.append({'node': node_id, 'state': 'rebuild', 'identity': identity,
                                'reason': 'block_cleared'})

            try:
                rows = self._invoke(node_id, node, record, plan, snapshot)
            except PlanStaleError:
                raise
            except Exception as exc:
                self.ledger.record(plan_id, node_id, 'failed', identity, run_no,
                                   error=type(exc).__name__ + ': ' + str(exc))
                reports.append({'node': node_id, 'state': 'failed', 'identity': identity,
                                'error': str(exc)})
                failed.add(node_id)
                continue

            output_digest = self.cache.put(identity, rows, {
                'transform_digest': record['transform_digest'],
                'params_digest': record['params_digest'],
                'inputs_digest': record['inputs_digest'],
            })
            self.ledger.record(plan_id, node_id, 'succeeded', identity, run_no,
                               output_digest=output_digest)
            reports.append({'node': node_id, 'state': 'executed', 'identity': identity,
                            'output_digest': output_digest})

        return self._summarize(plan_id, run_no, reports)

    def _invoke(self, node_id, node, record, plan, snapshot):
        input_rows = []
        for reference in node.inputs:
            if reference in self.graph.nodes:
                upstream_identity = plan['identities'][reference]
                entry = self.cache.get(upstream_identity)
                if entry is None:
                    raise RuntimeError('上游结果不可用：' + reference)
                input_rows.extend(entry['rows'])
            else:
                input_rows.extend(snapshot.read_partition(reference))
        rows = self.transform_executor(node.transform, input_rows, dict(node.params))
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise ValueError('变换输出必须是对象数组：' + node_id)
        return rows

    @staticmethod
    def _summarize(plan_id, run_no, reports):
        def pick(state):
            return [item['node'] for item in reports if item['state'] == state]
        succeeded = sorted(
            item['node'] for item in reports
            if item['state'] in ('executed', 'reused'))
        return {
            'plan_id': plan_id,
            'run_no': run_no,
            'completed': succeeded,
            'executed': pick('executed'),
            'reused': pick('reused'),
            'rebuilt': pick('rebuild'),
            'failed': pick('failed'),
            'blocked': pick('blocked'),
            'reports': reports,
        }
