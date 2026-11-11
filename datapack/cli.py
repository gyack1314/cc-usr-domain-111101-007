import argparse
import json
import sys
from pathlib import Path
from .records import inspect_bundle
from .storage import RevisionStore
from .flow import cli_flow
from .flow.errors import FlowError, GraphValidationError


def main():
    parser = argparse.ArgumentParser(description='数据包内容检查与内容寻址增量重算')
    commands = parser.add_subparsers(dest='command', required=True)
    p = commands.add_parser('inspect'); p.add_argument('bundle', type=Path)
    p = commands.add_parser('catalog'); p.add_argument('database', type=Path); p.add_argument('stream')
    cli_flow.add_subparsers(commands)
    args = parser.parse_args()
    try:
        if args.command == 'inspect':
            value = inspect_bundle(args.bundle)
        elif args.command == 'catalog':
            value = RevisionStore(args.database).latest(args.stream)
        else:
            value = cli_flow.dispatch(args)
        print(json.dumps(value, ensure_ascii=False, indent=2)); return 0
    except GraphValidationError as exc:
        print(json.dumps({'error': 'graph_validation_failed', 'issues': exc.as_dicts()},
                         ensure_ascii=False, indent=2), file=sys.stderr)
        return 3
    except (FlowError, ValueError, KeyError, OSError) as exc:
        print('输入资料或目录无效，请检查路径及字段说明：' + str(exc), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
