"""发布器：目标全部成功才发布，乐观并发核对预期前版本。

发布内容写入 RevisionStore（修订目录），每个发布目标一个 stream，
另有 release 总清单 stream。竞争发布时落败方抛 Conflict 且不动胜者指针，
其缓存与台账结果原样保留；旧发布修订永久可读。
"""
from ..storage import Conflict, RevisionStore
from .errors import PublishRejected


class Publisher:
    def __init__(self, graph, cache, ledger, revision_store):
        self.graph = graph
        self.cache = cache
        self.ledger = ledger
        self.store = revision_store if isinstance(revision_store, RevisionStore) \
            else RevisionStore(revision_store)

    def publish(self, plan, expected_release_revision=0):
        """expected_release_revision 为发布方看到的 release 清单当前版本（默认 0=尚无发布）。"""
        plan_id = plan['plan_id']
        required = self.graph.closure(
            [node_id for target_id in plan['targets']
             for node_id in self.graph.targets[target_id].nodes])
        states = {item['node']: item for item in self.ledger.plan_states(plan_id)}
        missing = [node_id for node_id in required
                   if node_id not in states or states[node_id]['state'] != 'succeeded']
        if missing:
            raise PublishRejected('计划目标依赖未全部成功，拒绝发布：' + ','.join(missing))

        release = {'plan_id': plan_id, 'graph_digest': plan['graph_digest'],
                   'targets': plan['targets'], 'artifacts': {}}
        target_rows = {}
        for target_id in plan['targets']:
            members = self.graph.closure(self.graph.targets[target_id].nodes)
            artifacts = {}
            for node_id in members:
                identity = plan['identities'][node_id]
                entry = self.cache.get(identity)
                if entry is None:
                    raise PublishRejected('成功节点的缓存缺失或损坏：' + node_id)
                artifacts[node_id] = {'identity': identity,
                                      'output_digest': entry['output_digest'],
                                      'rows': entry['rows']}
            release['artifacts'][target_id] = artifacts
            target_rows[target_id] = {node_id: artifacts[node_id]['rows']
                                      for node_id in artifacts}

        revision = self.store.put('release', expected_release_revision, release)
        for target_id in plan['targets']:
            self.store.put('target:' + target_id,
                           self._current_target_revision(target_id),
                           {'release_revision': revision, 'plan_id': plan_id,
                            'rows': target_rows[target_id]})
        return {'release_revision': revision, 'plan_id': plan_id,
                'targets': plan['targets']}

    def _current_target_revision(self, target_id):
        latest = self.store.latest('target:' + target_id)
        return latest['revision'] if latest else 0

    def read_release(self, revision=None):
        """读取发布内容；revision=None 取当前版本，旧版本号同样可读。"""
        if revision is None:
            latest = self.store.latest('release')
            if latest is None:
                raise KeyError('尚无发布')
            return latest
        entry = self.store.get('release', revision)
        return {'revision': revision, 'digest': entry['digest'], 'value': entry['value']}

    def current_release_revision(self):
        latest = self.store.latest('release')
        return latest['revision'] if latest else 0


__all__ = ['Publisher', 'PublishRejected', 'Conflict']
