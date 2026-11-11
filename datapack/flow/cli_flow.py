"""增量重算命令行：plan / diff / run / state / publish / release。

状态目录结构（均在 --workdir 下）：
  cache/    内容寻址缓存
  plans/    固定了图版本与输入摘要的计划
  ledger.sqlite3  每节点运行台账
  revisions.sqlite3 发布修订目录
"""
from pathlib import Path

from ..domain import REGISTRY
from ..storage import Conflict, read_json
from .cache import ContentCache
from .errors import FlowError, GraphValidationError
from .executor import Executor
from .ledger import Ledger
from .planner import Planner
from .publisher import Publisher
from .sources import StaticSource
from ..storage import RevisionStore


def sample_source(partitions_path):
    """从 samples/partitions.json（[{id, rows}] 清单）构造内存分区源。"""
    entries = read_json(partitions_path)
    if not isinstance(entries, list):
        raise ValueError('分区清单必须是对象数组')
    return StaticSource({entry['id']: entry['rows'] for entry in entries})


def _load_declaration(path):
    value = read_json(path)
    if not isinstance(value, list):
        raise ValueError('声明文件必须是节点/目标数组')
    return value


def _components(workdir, source):
    workdir = Path(workdir)
    cache = ContentCache(workdir / 'cache')
    planner = Planner(source, cache, workdir / 'plans', REGISTRY)
    ledger = Ledger(workdir / 'ledger.sqlite3')
    revisions = RevisionStore(workdir / 'revisions.sqlite3')
    return planner, cache, ledger, revisions


def _build_graph(planner, nodes_path, targets_path):
    return planner.build_validated_graph(_load_declaration(nodes_path),
                                         _load_declaration(targets_path))


def add_subparsers(parser):
    flow = parser.add_parser('plan', help='固定输入摘要与图版本，生成计划并给出失效解释')
    flow.add_argument('--workdir', required=True, type=Path)
    flow.add_argument('--partitions', required=True, type=Path)
    flow.add_argument('--nodes', required=True, type=Path)
    flow.add_argument('--targets', required=True, type=Path)

    diff = parser.add_parser('diff', help='解释指定计划各节点命中/失效原因')
    diff.add_argument('--workdir', required=True, type=Path)
    diff.add_argument('--partitions', required=True, type=Path)
    diff.add_argument('--plan', default=None)

    run = parser.add_parser('run', help='按计划执行，命中复用、失败阻断')
    run.add_argument('--workdir', required=True, type=Path)
    run.add_argument('--partitions', required=True, type=Path)
    run.add_argument('--nodes', required=True, type=Path)
    run.add_argument('--targets', required=True, type=Path)
    run.add_argument('--plan', default=None)

    state = parser.add_parser('state', help='查看计划每节点最近状态与尝试历史')
    state.add_argument('--workdir', required=True, type=Path)
    state.add_argument('--plan', default=None)

    publish = parser.add_parser('publish', help='全部目标依赖成功后更新发布指针')
    publish.add_argument('--workdir', required=True, type=Path)
    publish.add_argument('--partitions', required=True, type=Path)
    publish.add_argument('--nodes', required=True, type=Path)
    publish.add_argument('--targets', required=True, type=Path)
    publish.add_argument('--plan', default=None)
    publish.add_argument('--expected', type=int, default=None,
                         help='预期发布清单前版本，缺省取当前最新版本（竞争时应显式传入旧值）')

    release = parser.add_parser('release', help='读取发布内容（可指定旧修订号）')
    release.add_argument('--workdir', required=True, type=Path)
    release.add_argument('--revision', type=int, default=None)


def dispatch(args):
    if args.command == 'state':
        workdir = Path(args.workdir)
        ledger = Ledger(workdir / 'ledger.sqlite3')
        plan = _load_latest_plan(workdir, args.plan)
        return {'plan_id': plan['plan_id'], 'nodes': ledger.plan_states(plan['plan_id'])}

    if args.command == 'release':
        cache = ContentCache(Path(args.workdir) / 'cache')
        ledger = Ledger(Path(args.workdir) / 'ledger.sqlite3')
        revisions = RevisionStore(Path(args.workdir) / 'revisions.sqlite3')
        return Publisher(None, cache, ledger, revisions).read_release(args.revision)

    source = sample_source(args.partitions)
    planner, cache, ledger, revisions = _components(args.workdir, source)

    if args.command == 'plan':
        graph = _build_graph(planner, args.nodes, args.targets)
        plan = planner.make_plan(_load_declaration(args.nodes),
                                 _load_declaration(args.targets))
        return {'plan': plan, 'diff': planner.explain(plan, current_graph=graph)}

    plan = planner.load_plan(args.plan)

    if args.command == 'diff':
        return planner.explain(plan)

    if args.command == 'run':
        graph = _build_graph(planner, args.nodes, args.targets)
        if graph.digest != plan['graph_digest']:
            raise FlowError('图版本已变化，计划过期，请重新 plan')
        snapshot = planner.open_snapshot(plan)
        return Executor(graph, cache, ledger).run(plan, snapshot)

    if args.command == 'publish':
        graph = _build_graph(planner, args.nodes, args.targets)
        expected = args.expected
        if expected is None:
            latest = revisions.latest('release')
            expected = latest['revision'] if latest else 0
        return Publisher(graph, cache, ledger, revisions).publish(plan, expected)

    return None


def _load_latest_plan(workdir, plan_id):
    planner_path = Path(workdir) / 'plans'
    if plan_id is None:
        pointer = planner_path / 'latest'
        if not pointer.exists():
            raise KeyError('没有已保存的计划')
        plan_id = pointer.read_text(encoding='utf-8').strip()
    return read_json(planner_path / (plan_id + '.json'))
