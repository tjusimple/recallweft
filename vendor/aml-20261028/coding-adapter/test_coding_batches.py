import asyncio
import importlib.util
from pathlib import Path
import tempfile
from unittest.mock import AsyncMock, patch

from integrations.tencentdb_agent_memory_adapter.client import TencentMemoryClient, UpstreamError
from integrations.tencentdb_agent_memory_adapter.config import AdapterConfig

spec = importlib.util.spec_from_file_location('coding_adapter', Path(__file__).with_name('coding_adapter.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

async def main():
    with tempfile.TemporaryDirectory() as folder:
        client = m.BatchedClient(AdapterConfig(), Path(folder) / 'ledger.db')
        stored = []
        sent_sizes = []
        async def query(**kwargs):
            return list(stored)
        async def add(_self, body):
            sent_sizes.append(len(body['messages']))
            stored.extend(body['messages'])
            return {'accepted_ids': [str(i) for i in range(len(body['messages']))]}
        client.conversation_query = query
        body = {'team_id': 'test', 'agent_id': 'test', 'user_id': 'test', 'session_id': 'test',
                'messages': [{'role': 'user', 'content': 'message-' + str(i)} for i in range(251)]}
        m.REQUEST.set('request-1')
        with patch.object(TencentMemoryClient, 'add_conversation', add):
            result = await client.add_conversation(body)
            assert result['all_batches_confirmed'] and sent_sizes == [100, 100, 51]
            assert stored == body['messages']
            await client.add_conversation(body)
            assert sent_sizes == [100, 100, 51]
        # A timeout after commit must be reconciled without a second write.
        m.REQUEST.set('request-2')
        body2 = {**body, 'messages': [{'role': 'user', 'content': 'uncertain'}]}
        calls = []
        async def uncertain(_self, batch):
            calls.append(1)
            stored.extend(batch['messages'])
            raise UpstreamError('timeout after commit', outcome_uncertain=True)
        with patch.object(TencentMemoryClient, 'add_conversation', uncertain):
            try:
                await client.add_conversation(body2)
                raise AssertionError('expected uncertainty')
            except UpstreamError:
                pass
            await client.add_conversation(body2)
            assert len(calls) == 1
        # An unresolved/partial batch must fail closed, without a resend.
        m.REQUEST.set('request-3')
        body3 = {**body, 'messages': [{'role': 'user', 'content': 'not-committed'}]}
        async def not_committed(_self, batch):
            calls.append(1)
            raise UpstreamError('transport lost', outcome_uncertain=True)
        with patch.object(TencentMemoryClient, 'add_conversation', not_committed):
            before = len(calls)
            for _ in range(2):
                try:
                    await client.add_conversation(body3)
                    raise AssertionError('must not acknowledge an unresolved batch')
                except UpstreamError:
                    pass
            assert len(calls) == before + 1
        await client.close()
    print('6 batching protections passed: boundaries, losslessness, repeat, post-commit timeout, partial failure, no false success')

asyncio.run(main())
