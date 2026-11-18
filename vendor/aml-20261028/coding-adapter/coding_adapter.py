"""Lossless >100-message Add transport for TencentDB's existing v3 API.

Only the evaluation wrapper changes. Core, retrieval, model and benchmark stay
unchanged. Each 100-message batch is durably reserved before transmission. An
uncertain batch is confirmed by L0 readback or fails closed, never blindly sent
again. A successful Add is acknowledged only after all batches are confirmed.
"""
import contextvars
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sqlite3

from integrations.tencentdb_agent_memory_adapter.app import create_app
from integrations.tencentdb_agent_memory_adapter.client import TencentMemoryClient, UpstreamError
from integrations.tencentdb_agent_memory_adapter.config import AdapterConfig
from integrations.tencentdb_agent_memory_adapter.service import AdapterService
from integrations.tencentdb_agent_memory_adapter.state import AdapterState

REQUEST = contextvars.ContextVar('tencentdb_request_id')

def counts(messages):
    return Counter(hashlib.sha256(json.dumps([m.get('role'), m.get('content')],
                   ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
                   for m in messages)

def contains(current, expected):
    return all(current[k] >= n for k, n in expected.items())

class BatchedClient(TencentMemoryClient):
    def __init__(self, config, ledger_path):
        super().__init__(config)
        self.ledger = sqlite3.connect(ledger_path)
        self.ledger.execute('pragma journal_mode=WAL')
        self.ledger.execute('''create table if not exists batches (
            request_id text not null, batch_index integer not null,
            payload_hash text not null, expected_counts text not null,
            status text not null, primary key(request_id,batch_index))''')
        self.ledger.commit()

    async def close(self):
        self.ledger.close()
        await super().close()

    async def add_conversation(self, payload):
        request_id = REQUEST.get()
        messages = payload['messages']
        isolation = {k: payload[k] for k in ['team_id', 'agent_id', 'user_id']}
        accepted = []
        for offset in range(0, len(messages), 100):
            index = offset // 100
            batch = {**payload, 'messages': messages[offset:offset + 100]}
            digest = hashlib.sha256(json.dumps(batch, ensure_ascii=False,
                      sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            row = self.ledger.execute('select payload_hash,expected_counts,status from batches where request_id=? and batch_index=?', (request_id, index)).fetchone()
            if row:
                if row[0] != digest:
                    raise UpstreamError('batch identity conflict', status_code=409)
                if row[2] == 'confirmed':
                    continue
                if row[2] == 'pending':
                    current = counts(await self.conversation_query(isolation=isolation, session_id=payload['session_id']))
                    if contains(current, json.loads(row[1])):
                        self.ledger.execute("update batches set status='confirmed' where request_id=? and batch_index=?", (request_id, index))
                        self.ledger.commit()
                        continue
                    raise UpstreamError('uncertain batch is not fully confirmed by readback; refusing duplicate write', outcome_uncertain=True)
            baseline = counts(await self.conversation_query(isolation=isolation, session_id=payload['session_id']))
            expected = baseline + counts(batch['messages'])
            self.ledger.execute("insert into batches values (?,?,?,?, 'pending') on conflict(request_id,batch_index) do update set expected_counts=excluded.expected_counts,status='pending'", (request_id, index, digest, json.dumps(dict(expected))))
            self.ledger.commit()
            try:
                response = await super().add_conversation(batch)
            except UpstreamError as exc:
                if not exc.outcome_uncertain:
                    self.ledger.execute("update batches set status='rejected' where request_id=? and batch_index=?", (request_id, index))
                    self.ledger.commit()
                raise
            # HTTP success confirms all records, following TencentDB's native
            # accepted_ids/total_count acknowledgement, not a partial batch.
            if len(response.get('accepted_ids', [])) != len(batch['messages']):
                raise UpstreamError('upstream acknowledged an incomplete batch', outcome_uncertain=True)
            self.ledger.execute("update batches set status='confirmed' where request_id=? and batch_index=?", (request_id, index))
            self.ledger.commit()
            accepted.extend(response['accepted_ids'])
        return {'accepted_ids': accepted, 'all_batches_confirmed': True}

class BatchedService(AdapterService):
    async def add(self, request):
        token = REQUEST.set(request.request_id)
        try:
            return await super().add(request)
        finally:
            REQUEST.reset(token)

    async def _reconcile_pending_add(self, **kwargs):
        # The per-batch ledger reconciles against its saved pre-write baseline.
        # A whole-request content match could confuse identical older messages
        # with this request, so do not use the legacy whole-session shortcut.
        return False

def make_app():
    config = AdapterConfig.from_env()
    state = AdapterState(config.state_db_path)
    client = BatchedClient(config, config.state_db_path.with_suffix('.batches.db'))
    service = BatchedService(config=config, state=state, client=client)
    return create_app(config=config, service=service)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run('coding_adapter:make_app', host='0.0.0.0', port=int(os.environ['LDBD_ADAPTER_PORT']), factory=True)
