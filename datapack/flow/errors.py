"""增量调度相关异常类型。"""


class FlowError(Exception):
    """内容寻址增量重算的基础异常。"""


class GraphValidationError(FlowError):
    """图在执行前校验失败，issues 保存全部问题，不依赖执行顺序偶然暴露。"""

    def __init__(self, issues):
        self.issues = list(issues)
        detail = '；'.join(issue.message for issue in self.issues)
        super().__init__('图校验失败：' + detail)

    def as_dicts(self):
        return [issue.as_dict() for issue in self.issues]


class GraphIssue:
    def __init__(self, kind, message, node=None):
        self.kind = kind
        self.node = node
        self.message = message

    def as_dict(self):
        return {'kind': self.kind, 'node': self.node, 'message': self.message}

    def __eq__(self, other):
        return isinstance(other, GraphIssue) and (self.kind, self.node, self.message) == (
            other.kind, other.node, other.message)


class PlanStaleError(FlowError):
    """计划固定的输入摘要或图版本已与当前资料不一致。"""


class CacheCorrupted(FlowError):
    """缓存对象无法通过内容校验（内部异常，正常流程会被识别为缓存未命中）。"""


class PublishRejected(FlowError):
    """计划目标未全部成功，拒绝发布。"""
