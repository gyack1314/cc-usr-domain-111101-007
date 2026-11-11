"""内容寻址增量重算端到端演示（仅标准库，纯本地表格运算）。

依次演示：
  1. 基线 plan/run/publish，并打印差异解释
  2. 修改一个分区：只让相关下游失效，无关节点零调用
  3. 只改参数：分区未动，仅参数变化的节点及下游重算
  4. 破坏缓存：损坏被识别、隔离并重建
  5. 节点中断恢复：失败节点阻断下游，修复后重试，完整节点不重复运行
  6. 竞争发布：落败方保留结果、不覆盖胜者；旧发布修订一直可读
  7. 计划过期：计划固定后修改输入，run 直接拒绝，不读新文件
所有工作产物放在临时目录，运行结束自动清理。用 python -I 运行。
"""
import shutil
import sys
import tempfile
import traceback
from pathlib import Path

# 支持 python -I 运行：隔离模式不自动加入当前工作目录，这里显式注入项目根。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datapack.domain import REGISTRY, execute_transform
from datapack.flow.cache import ContentCache
from datapack.flow.errors import PlanStaleError, PublishRejected
from datapack.flow.executor import Executor
from datapack.flow.ledger import Ledger
from datapack.flow.planner import Planner
from datapack.flow.publisher import Publisher
from datapack.flow.sources import StaticSource
from datapack.storage import Conflict, RevisionStore

NODES = [
    {'id': 'east_only', 'transform': 'identity', 'inputs': ['east/2026-11-01'], 'params': {}},
    {'id': 'all_summary', 'transform': 'sum_by',
     'inputs': ['east_only', 'west/2026-11-01'], 'params': {'key': 'city', 'field': 'distance'}},
    {'id': 'keys', 'transform': 'project', 'inputs': ['all_summary'], 'params': {'fields': ['city']}},
]
TARGETS = [{'id': 'daily', 'nodes': ['all_summary', 'keys']}]

BASE_PARTITIONS = {
    'east/2026-11-01': [{'city': 'east', 'distance': 10}, {'city': 'east', 'distance': 20}],
    'west/2026-11-01': [{'city': 'west', 'distance': 40}],
    'east/2026-11-02': [{'city': 'east', 'distance': 15}],
}


class CheckFailed(AssertionError):
    pass


def check(condition, message):
    if not condition:
        raise CheckFailed(message)
    print('    [通过] ' + message)


def heading(title):
    print('\n' + '=' * 72 + '\n' + title + '\n' + '=' * 72)


class Workbench:
    def __init__(self, root):
        self.root = Path(root)
        self.source = StaticSource(BASE_PARTITIONS)
        self.cache = ContentCache(self.root / 'cache')
        self.planner = Planner(self.source, self.cache, self.root / 'plans', REGISTRY)
        self.ledger = Ledger(self.root / 'ledger.sqlite3')
        self.revisions = RevisionStore(self.root / 'revisions.sqlite3')
        self.calls = []

    def counting_executor(self, name, rows, params):
        self.calls.append(name)
        return execute_transform(name, rows, params)

    def graph(self, nodes=None, targets=None):
        return self.planner.build_validated_graph(nodes or NODES, targets or TARGETS)

    def plan(self, nodes=None, targets=None):
        return self.planner.make_plan(nodes or NODES, targets or TARGETS)

    def run(self, plan, graph=None, executor=None):
        graph = graph or self.graph()
        executor = executor or Executor(graph, self.cache, self.ledger, self.counting_executor)
        return executor.run(plan, self.planner.open_snapshot(plan))

    def publisher(self, graph=None):
        return Publisher(graph or self.graph(), self.cache, self.ledger, self.revisions)


def print_diff(diff):
    for item in diff['nodes']:
        reasons = '；'.join(item['reasons'])
        print('    - {node}: {status}（{reasons}）'.format(node=item['node'],
                                                           status=item['status'], reasons=reasons))


def main():
    root = Path(tempfile.mkdtemp(prefix='datapack-demo-'))
    print('工作目录：' + str(root))
    bench = Workbench(root)
    try:
        heading('场景 1：基线计划、差异解释、执行与发布')
        graph = bench.graph()
        plan1 = bench.plan()
        print('  计划固定图版本 {0} 与 {1} 个输入分区摘要'.format(
            plan1['graph_digest'][:12], len(plan1['source_digests'])))
        diff1 = bench.planner.explain(plan1, current_graph=graph)
        print_diff(diff1)
        bench.calls.clear()
        report1 = bench.run(plan1)
        print('  执行节点：' + ', '.join(report1['executed']))
        check(sorted(bench.calls) == ['identity', 'project', 'sum_by'], '三个节点各真实调用一次')
        publish1 = bench.publisher().publish(plan1, 0)
        check(publish1['release_revision'] == 1, '首次发布生成修订 1')

        heading('场景 2：修改一个分区（east/2026-11-01 距离 20→25）')
        bench.source._partitions['east/2026-11-01'] = [
            {'city': 'east', 'distance': 10}, {'city': 'east', 'distance': 25}]
        plan2 = bench.plan()
        diff2 = bench.planner.explain(plan2, plan1, current_graph=bench.graph())
        print_diff(diff2)
        check(diff2['changed_sources'] == ['east/2026-11-01'], '只有被引用的 east 分区出现在变更清单')
        bench.calls.clear()
        report2 = bench.run(plan2)
        check(sorted(report2['executed']) == ['all_summary', 'east_only', 'keys'],
              '读取该分区的节点及其下游全部重算')
        bench.publisher().publish(plan2, 1)

        heading('场景 3：新增无关分区不失效；只改 west 分区时 east_only 零调用')
        bench.source._partitions['north/2026-11-01'] = [{'city': 'north', 'distance': 99}]
        plan_extra = bench.plan()
        diff_extra = bench.planner.explain(plan_extra, plan2, current_graph=bench.graph())
        check(all(item['status'] == 'cache_hit' for item in diff_extra['nodes']),
              '目录新增未引用分区，全部节点保持命中')
        bench.source._partitions['west/2026-11-01'] = [{'city': 'west', 'distance': 41}]
        plan3 = bench.plan()
        diff3 = bench.planner.explain(plan3, plan_extra, current_graph=bench.graph())
        statuses = {item['node']: item['status'] for item in diff3['nodes']}
        check(statuses['east_only'] == 'cache_hit' and statuses['all_summary'] == 'invalidated',
              'west 分区变化只使 all_summary/keys 失效，east_only 命中')
        bench.calls.clear()
        bench.run(plan3)
        check('identity' not in bench.calls and bench.calls.count('sum_by') == 1,
              'east_only 未真实调用，sum_by 调用一次，实际调用可被注入执行器观察')
        bench.publisher().publish(plan3, 2)

        heading('场景 4：只改参数（all_summary 增加参数），分区内容不变')
        changed_nodes = [dict(node) for node in NODES]
        changed_nodes[1] = dict(changed_nodes[1],
                                params={'key': 'city', 'field': 'distance', 'round': 'v2'})
        graph4 = bench.graph(changed_nodes)
        plan4 = bench.plan(changed_nodes)
        diff4 = bench.planner.explain(plan4, plan3, current_graph=graph4)
        statuses4 = {item['node']: item['status'] for item in diff4['nodes']}
        check(statuses4['east_only'] == 'cache_hit', 'east_only 身份不变，命中缓存')
        check(statuses4['all_summary'] == 'invalidated', '参数变化使 all_summary 失效')
        reasons = next(item for item in diff4['nodes'] if item['node'] == 'all_summary')['reasons']
        check(any('参数' in reason for reason in reasons), '失效原因明确指向规范化参数变化')
        bench.run(plan4, graph4)

        heading('场景 5：破坏缓存，识别损坏并隔离重建')
        identity = plan4['identities']['keys']
        cache_path = bench.cache._path(identity)
        cache_path.write_text('{"broken": true}', encoding='utf-8')
        diff5 = bench.planner.explain(plan4, current_graph=graph4)
        check(next(item for item in diff5['nodes']
                   if item['node'] == 'keys')['status'] == 'cache_corrupted',
              '差异解释把 keys 标记为 cache_corrupted')
        cache_path.write_text('{"broken": true}', encoding='utf-8')
        bench.calls.clear()
        report5 = bench.run(plan4, graph4)
        check(report5['executed'] == ['keys'], '仅损坏的 keys 节点重新执行')
        quarantined = list((bench.root / 'cache' / 'quarantine').glob('*.json'))
        check(bool(quarantined), '残文件已移入 quarantine 隔离目录')

        heading('场景 6：节点中断后恢复（失败阻断下游，修复后重试不重复运行完整节点）')
        # 用新输入产生新的节点身份，确保 flaky 执行器真正被调用而不是命中旧缓存
        bench.source._partitions['west/2026-11-01'] = [{'city': 'west', 'distance': 42}]
        flaky = {'fail': True}

        def flaky_executor(name, rows, params):
            if name == 'sum_by' and flaky['fail']:
                raise RuntimeError('模拟节点中断')
            return execute_transform(name, rows, params)

        plan6 = bench.plan(changed_nodes)
        report6 = bench.run(plan6, graph4,
                            Executor(graph4, bench.cache, bench.ledger, flaky_executor))
        check(report6['failed'] == ['all_summary'], 'all_summary 记录为 failed')
        check(report6['blocked'] == ['keys'], '下游 keys 被阻断，不进入本次成功清单')
        check('keys' not in report6['completed'], '被阻断节点的上次成功内容不冒充本次结果')
        try:
            bench.publisher(graph4).publish(plan6, 99)
            raise CheckFailed('发布门禁本应拒绝不完整计划')
        except PublishRejected:
            print('    [通过] 存在失败/阻断节点时发布被拒绝')
        flaky['fail'] = False
        report7 = bench.run(plan6, graph4,
                            Executor(graph4, bench.cache, bench.ledger, flaky_executor))
        check(sorted(report7['executed']) == ['all_summary', 'keys'], '修复后失败节点与阻断节点补跑成功')
        check(report7['reused'] == ['east_only'], '已完整的 east_only 仅复用、未重复运行')
        history = bench.ledger.attempts(plan6['plan_id'], 'all_summary')
        check([item['state'] for item in history] == ['failed', 'succeeded'],
              '台账保留 failed→succeeded 的可复查历史')
        # 模拟进程重启：全新对象连接同一持久目录
        restarted_cache = ContentCache(bench.root / 'cache')
        restarted_ledger = Ledger(bench.root / 'ledger.sqlite3')
        report8 = Executor(graph4, restarted_cache, restarted_ledger,
                           bench.counting_executor).run(plan6, bench.planner.open_snapshot(plan6))
        check(report8['executed'] == [], '重启后重试同一计划，完整节点零重复运行')

        heading('场景 7：竞争发布与旧发布可读性')
        bench.publisher(graph4).publish(plan6, 3)
        # 另一个维护者拿着更旧的预期版本 0 发布 plan4（内容不同）
        try:
            bench.publisher(graph4).publish(plan4, 0)
            raise CheckFailed('竞争发布本应失败')
        except Conflict:
            print('    [通过] 预期前版本 0 与现版不符，落败发布抛 Conflict')
        current = bench.publisher(graph4).read_release()
        check(current['value']['plan_id'] == plan6['plan_id'], '胜者发布指针未被覆盖')
        check(bench.cache.get(plan4['identities']['keys']) is not None,
              '落败计划的缓存结果原样保留')
        check(bench.ledger.state(plan4['plan_id'], 'keys')['state'] == 'succeeded',
              '落败计划的成功台账仍然可查')
        for revision in (1, 2, 3, 4):
            entry = bench.publisher(graph4).read_release(revision)
            check(entry['revision'] == revision and bool(entry['value']['artifacts']),
                  '旧发布修订 {0} 一直可读'.format(revision))
        first = bench.publisher(graph4).read_release(1)['value']
        check(first['artifacts']['daily']['all_summary']['rows']
              == [{'city': 'east', 'distance': 30}, {'city': 'west', 'distance': 40}],
              '修订 1 保留最初的汇总结果（east=30, west=40）')

        heading('场景 8：计划过期保护（plan 之后输入变化，run 拒绝读取新文件）')
        frozen_plan = bench.plan()
        bench.source._partitions['east/2026-11-01'] = [{'city': 'east', 'distance': 1}]
        observed = []
        try:
            bench.run(frozen_plan, executor=Executor(
                bench.graph(), bench.cache, bench.ledger,
                lambda name, rows, params: observed.append(name) or execute_transform(name, rows, params)))
            raise CheckFailed('过期计划本应被拒绝')
        except PlanStaleError as exc:
            print('    [通过] run 抛出 PlanStaleError：' + str(exc))
        check(observed == [], '没有任何变换被调用，新文件未被悄悄读取')

        heading('全部场景通过')
    finally:
        shutil.rmtree(root, ignore_errors=True)
        print('\n已清理临时目录：' + str(root))


if __name__ == '__main__':
    try:
        main()
    except CheckFailed:
        traceback.print_exc()
        raise SystemExit(1)
