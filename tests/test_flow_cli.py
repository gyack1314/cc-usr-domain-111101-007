import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_cli(*arguments):
    return subprocess.run([sys.executable, '-m', 'datapack.cli', *arguments],
                          cwd=ROOT, capture_output=True, text=True)


class FlowCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workdir = Path(self.tmp.name) / 'state'

    def tearDown(self):
        self.tmp.cleanup()

    def _flow(self, command, *extra):
        arguments = [command, '--workdir', str(self.workdir)]
        if command not in ('state', 'release'):
            arguments += ['--partitions', 'samples/partitions.json',
                          '--nodes', 'samples/nodes.json', '--targets', 'samples/targets.json']
        return run_cli(*arguments, *extra)

    def test_plan_run_retry_publish_release(self):
        plan_result = self._flow('plan')
        self.assertEqual(plan_result.returncode, 0, plan_result.stderr)
        plan = json.loads(plan_result.stdout)['plan']

        first = self._flow('run')
        self.assertEqual(first.returncode, 0, first.stderr)
        first_body = json.loads(first.stdout)
        self.assertEqual(len(first_body['executed']), 3)

        second = self._flow('run')
        second_body = json.loads(second.stdout)
        self.assertEqual(second_body['executed'], [])
        self.assertEqual(len(second_body['reused']), 3)

        state = self._flow('state')
        states = json.loads(state.stdout)['nodes']
        self.assertTrue(all(item['state'] == 'succeeded' and item['attempts'] == 1
                            for item in states))

        publish = self._flow('publish')
        self.assertEqual(json.loads(publish.stdout)['release_revision'], 1)

        release = self._flow('release', '--revision', '1')
        body = json.loads(release.stdout)
        self.assertEqual(body['value']['plan_id'], plan['plan_id'])
        self.assertEqual(body['value']['artifacts']['daily']['all_summary']['rows'],
                         [{'city': 'east', 'distance': 30}, {'city': 'west', 'distance': 40}])

    def test_cyclic_graph_rejected_before_execution(self):
        result = run_cli(
            'plan', '--workdir', str(self.workdir),
            '--partitions', 'samples/partitions.json',
            '--nodes', 'examples/cyclic-nodes.json',
            '--targets', 'samples/targets.json')
        self.assertEqual(result.returncode, 3)
        payload = json.loads(result.stderr)
        self.assertEqual(payload['error'], 'graph_validation_failed')
        self.assertIn('cycle', [issue['kind'] for issue in payload['issues']])

    def test_stale_plan_run_is_failure(self):
        self._flow('plan')
        partition_path = ROOT / 'samples' / 'partitions.json'
        original = partition_path.read_text(encoding='utf-8')
        try:
            self._flow('run')
            payload = json.loads(original)
            payload[0]['rows'][0]['distance'] = 999
            partition_path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
            stale = self._flow('run')
            self.assertEqual(stale.returncode, 2)
            self.assertIn('计划过期', stale.stderr)
        finally:
            partition_path.write_text(original, encoding='utf-8')


if __name__ == '__main__':
    unittest.main()
