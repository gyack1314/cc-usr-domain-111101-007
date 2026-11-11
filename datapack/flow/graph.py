"""DAG 声明、图校验与图版本摘要。

图由节点（纯变换）、目标和外部输入分区组成。所有结构性问题一次性收集，
缺边、循环、重复输出和未知分区引用在任何执行之前报错。
"""
from dataclasses import dataclass, field

from ..domain import transform_digest
from ..storage import fingerprint
from .errors import GraphIssue


@dataclass(frozen=True)
class Node:
    id: str
    transform: str
    inputs: tuple
    params: dict

    def as_dict(self):
        return {'id': self.id, 'transform': self.transform,
                'inputs': list(self.inputs), 'params': self.params}


@dataclass(frozen=True)
class Target:
    id: str
    nodes: tuple

    def as_dict(self):
        return {'id': self.id, 'nodes': list(self.nodes)}


@dataclass(frozen=True)
class Graph:
    nodes: dict
    targets: dict
    digest: str
    issues: tuple = field(default_factory=tuple)

    def node_ids(self):
        return set(self.nodes)

    def closure(self, node_ids):
        """返回这些节点（含目标传递依赖）的闭包，按拓扑序排列。"""
        order = self.topological_order()
        wanted = set()
        stack = list(node_ids)
        while stack:
            current = stack.pop()
            if current in wanted:
                continue
            wanted.add(current)
            stack.extend(reference for reference in self.nodes[current].inputs
                         if reference in self.nodes)
        return [node_id for node_id in order if node_id in wanted]

    def topological_order(self):
        """Kahn 拓扑序；校验阶段已保证无环，这里防御性再检一次。"""
        remaining = set(self.nodes)
        deps = {node_id: set(node.inputs) & remaining
                for node_id, node in self.nodes.items()}
        ready = sorted(node_id for node_id in remaining if not deps[node_id])
        order = []
        while ready:
            current = ready.pop(0)
            order.append(current)
            remaining.remove(current)
            unlocked = []
            for node_id in remaining:
                deps[node_id].discard(current)
                if not deps[node_id] and node_id not in ready:
                    unlocked.append(node_id)
            ready.extend(unlocked)
            ready.sort()
        if len(order) != len(self.nodes):
            raise ValueError('图存在循环，无法排序')
        return order


def _as_list(raw, label, issues):
    if not isinstance(raw, list):
        issues.append(GraphIssue('invalid_declaration', label + '必须是数组'))
        return []
    return raw


def build_graph(raw_nodes, raw_targets, registered_transforms, source_partitions):
    """根据原始 JSON 声明构图，返回 Graph；问题全部挂在 issues 上，不抛异常。

    source_partitions 是当前可用外部输入分区 id 集合，用于未知分区引用校验。
    """
    issues = []
    node_ids = set()
    nodes = {}
    entries = _as_list(raw_nodes, 'nodes', issues)
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            issues.append(GraphIssue('invalid_declaration', '第' + str(index + 1) + '个节点必须是对象'))
            continue
        node_id = entry.get('id')
        if not isinstance(node_id, str) or not node_id:
            issues.append(GraphIssue('invalid_declaration', '第' + str(index + 1) + '个节点缺少有效id'))
            continue
        duplicate = node_id in node_ids
        if duplicate:
            issues.append(GraphIssue('duplicate_output', '节点输出重复：' + node_id, node_id))
        transform = entry.get('transform')
        inputs = entry.get('inputs', [])
        params = entry.get('params', {})
        if not isinstance(transform, str) or not transform:
            issues.append(GraphIssue('invalid_declaration', '节点 ' + node_id + ' 缺少有效transform', node_id))
        elif transform not in registered_transforms:
            issues.append(GraphIssue('unknown_transform', '节点 ' + node_id + ' 引用未登记变换 ' + str(transform), node_id))
        if not isinstance(inputs, list) or not all(isinstance(item, str) and item for item in inputs):
            issues.append(GraphIssue('invalid_declaration', '节点 ' + node_id + ' 的inputs必须是非空字符串数组', node_id))
            inputs = []
        if not isinstance(params, dict):
            issues.append(GraphIssue('invalid_declaration', '节点 ' + node_id + ' 的params必须是对象', node_id))
            params = {}
        if duplicate:
            continue
        node_ids.add(node_id)
        nodes[node_id] = Node(node_id, transform if isinstance(transform, str) else '',
                              tuple(inputs), dict(params))

    targets = {}
    target_entries = _as_list(raw_targets, 'targets', issues)
    target_ids = set()
    for index, entry in enumerate(target_entries):
        if not isinstance(entry, dict) or not isinstance(entry.get('id'), str) or not entry['id']:
            issues.append(GraphIssue('invalid_declaration', '第' + str(index + 1) + '个目标缺少有效id'))
            continue
        target_id = entry['id']
        if target_id in target_ids:
            issues.append(GraphIssue('duplicate_output', '目标重复：' + target_id, target_id))
            continue
        members = entry.get('nodes', [])
        if not isinstance(members, list) or not all(isinstance(item, str) and item for item in members):
            issues.append(GraphIssue('invalid_declaration', '目标 ' + target_id + ' 的nodes必须是字符串数组', target_id))
            continue
        target_ids.add(target_id)
        for member in members:
            if member not in nodes:
                issues.append(GraphIssue('missing_edge', '目标 ' + target_id + ' 引用不存在节点 ' + member, member))
        targets[target_id] = Target(target_id, tuple(members))

    for node_id, node in nodes.items():
        for reference in node.inputs:
            if reference in nodes or reference in source_partitions:
                continue
            issues.append(GraphIssue('missing_partition',
                                     '节点 ' + node_id + ' 引用不存在分区或节点 ' + reference, node_id))

    _detect_cycles(nodes, issues)

    digest_input = {
        'nodes': [node.as_dict() for node_id, node in sorted(nodes.items())],
        'targets': [target.as_dict() for target_id, target in sorted(targets.items())],
    }
    return Graph(nodes, targets, fingerprint(digest_input), tuple(issues))


def _detect_cycles(nodes, issues):
    """对节点间引用做 DFS 三色标记，找出所有参与循环的节点（只在节点集合内追踪）。"""
    state = {node_id: 0 for node_id in nodes}
    stack = []
    reported = set()

    def visit(node_id):
        state[node_id] = 1
        stack.append(node_id)
        for reference in nodes[node_id].inputs:
            if reference not in nodes:
                continue
            if state[reference] == 0:
                visit(reference)
            elif state[reference] == 1 and reference not in reported:
                cycle = stack[stack.index(reference):] + [reference]
                issues.append(GraphIssue('cycle', '节点存在循环：' + ' -> '.join(cycle), reference))
                reported.update(cycle)
        stack.pop()
        state[node_id] = 2

    for node_id in sorted(nodes):
        if state[node_id] == 0:
            visit(node_id)


def node_transform_digest(node):
    """变换代码摘要；未登记时由校验阶段拦截，这里兜底抛错。"""
    try:
        return transform_digest(node.transform)
    except ValueError:
        raise ValueError('变换未登记：' + node.transform) from None
