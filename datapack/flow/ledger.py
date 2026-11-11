"""运行台账：每节点完成记录持久化到 SQLite，重启后可复查。

node_runs 记录每个计划下每个节点最近一次状态及成功输出摘要；
node_attempts 追加保留每次尝试（成功/失败/阻断），形成可复查历史。
状态取值：succeeded / failed / blocked。
"""
import sqlite3
import time
from pathlib import Path

TERMINAL_STATES = ('succeeded', 'failed', 'blocked')


class Ledger:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                'CREATE TABLE IF NOT EXISTS node_runs (plan_id TEXT NOT NULL,node_id TEXT NOT NULL,'
                'state TEXT NOT NULL,identity TEXT NOT NULL,output_digest TEXT,error TEXT,'
                'attempts INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL,'
                'PRIMARY KEY(plan_id,node_id))')
            conn.execute(
                'CREATE TABLE IF NOT EXISTS node_attempts (id INTEGER PRIMARY KEY AUTOINCREMENT,'
                'plan_id TEXT NOT NULL,node_id TEXT NOT NULL,state TEXT NOT NULL,identity TEXT NOT NULL,'
                'output_digest TEXT,error TEXT,run_no INTEGER NOT NULL,created_at TEXT NOT NULL)')

    def state(self, plan_id, node_id):
        with sqlite3.connect(self.path) as conn:
            row = conn.execute(
                'SELECT state,identity,output_digest,error,attempts FROM node_runs '
                'WHERE plan_id=? AND node_id=?', (plan_id, node_id)).fetchone()
        if row is None:
            return None
        return {'state': row[0], 'identity': row[1], 'output_digest': row[2],
                'error': row[3], 'attempts': row[4]}

    def record(self, plan_id, node_id, state, identity, run_no, output_digest=None, error=None):
        if state not in TERMINAL_STATES:
            raise ValueError('未知节点状态：' + state)
        stamp = time.strftime('%Y-%m-%dT%H:%M:%S')
        with sqlite3.connect(self.path) as conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute(
                'SELECT attempts FROM node_runs WHERE plan_id=? AND node_id=?',
                (plan_id, node_id)).fetchone()
            attempts = (row[0] if row else 0) + 1
            conn.execute(
                'INSERT INTO node_runs VALUES(?,?,?,?,?,?,?,?) '
                'ON CONFLICT(plan_id,node_id) DO UPDATE SET state=excluded.state,'
                'identity=excluded.identity,output_digest=excluded.output_digest,'
                'error=excluded.error,attempts=excluded.attempts,updated_at=excluded.updated_at',
                (plan_id, node_id, state, identity, output_digest, error, attempts, stamp))
            conn.execute(
                'INSERT INTO node_attempts(plan_id,node_id,state,identity,output_digest,error,run_no,created_at)'
                ' VALUES(?,?,?,?,?,?,?,?)',
                (plan_id, node_id, state, identity, output_digest, error, run_no, stamp))

    def ensure_succeeded(self, plan_id, node_id, identity, output_digest, run_no):
        """缓存命中时补齐本计划的成功状态。

        已是 succeeded 则幂等无操作（不追加尝试事件，重试不重复运行）；
        此前为 failed/blocked 则翻转为 succeeded，并追加一条成功事件保留轨迹。
        """
        with sqlite3.connect(self.path) as conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute(
                'SELECT state,attempts FROM node_runs WHERE plan_id=? AND node_id=?',
                (plan_id, node_id)).fetchone()
            if row is not None and row[0] == 'succeeded':
                return False
            stamp = time.strftime('%Y-%m-%dT%H:%M:%S')
            attempts = 1 if row is None else row[1] + 1
            conn.execute(
                'INSERT INTO node_runs VALUES(?,?,?,?,?,?,?,?) '
                'ON CONFLICT(plan_id,node_id) DO UPDATE SET state=excluded.state,'
                'identity=excluded.identity,output_digest=excluded.output_digest,'
                'error=excluded.error,attempts=excluded.attempts,updated_at=excluded.updated_at',
                (plan_id, node_id, 'succeeded', identity, output_digest, None, attempts, stamp))
            conn.execute(
                'INSERT INTO node_attempts(plan_id,node_id,state,identity,output_digest,error,run_no,created_at)'
                ' VALUES(?,?,?,?,?,?,?,?)',
                (plan_id, node_id, 'succeeded', identity, output_digest, None, run_no, stamp))
            return True

    def next_run_no(self, plan_id):
        with sqlite3.connect(self.path) as conn:
            row = conn.execute('SELECT COALESCE(MAX(run_no),0)+1 FROM node_attempts WHERE plan_id=?',
                               (plan_id,)).fetchone()
        return row[0]

    def plan_states(self, plan_id):
        with sqlite3.connect(self.path) as conn:
            rows = conn.execute(
                'SELECT node_id,state,identity,output_digest,error,attempts FROM node_runs '
                'WHERE plan_id=? ORDER BY node_id', (plan_id,)).fetchall()
        return [{'node': row[0], 'state': row[1], 'identity': row[2], 'output_digest': row[3],
                 'error': row[4], 'attempts': row[5]} for row in rows]

    def attempts(self, plan_id, node_id):
        with sqlite3.connect(self.path) as conn:
            rows = conn.execute(
                'SELECT state,output_digest,error,run_no,created_at FROM node_attempts '
                'WHERE plan_id=? AND node_id=? ORDER BY id', (plan_id, node_id)).fetchall()
        return [{'state': row[0], 'output_digest': row[1], 'error': row[2],
                 'run_no': row[3], 'at': row[4]} for row in rows]
