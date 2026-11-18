# TencentDB fixed entrant deployment (2026-10-28)

Repository: https://github.com/TencentCloud/TencentDB-Agent-Memory
Pinned commit: 42c6711c218b40d38d1d6b0ed4767bcc6069cbfd (default branch feat/server_team at freeze).

One stable synchronous Bearer-authenticated Add/Search API serves textual, coding and multimodal tracks, using the same frozen implementation. Every new evaluation starts with its own public user/session identities; no historical evaluation data/results are imported.

Runtime: Node.js 24.19.0, Python 3.10. Memory extraction and image descriptions use gpt-4o-mini. Native BM25 is retained, embedding and extra decision service are disabled. The image-description-v1 prompt runs on the image alone, not questions/options; this is a caption adaptation, not native image memory.

The attached API wrapper retains source text/timestamps, losslessly splits long messages at 8192 characters and transports batches of at most 100 native messages. The wrapper accepts at most 65536 expanded messages and 48 MiB JSON per request; oversized requests fail rather than silently truncate. Search uses the wrapper's fixed source fusion and 2048-character head/tail query handling. Request hashes, batch acknowledgements, unknown-write readback/fail-closed protections and caption provenance are durable. API Add/Search slots: 4/8, caption slots 2. These are participant service limits, not changes to AML evaluation concurrency or scoring.

The included operational patch adds SQLite WAL/FULL storage for exact scoped pipeline state/cursors, buffers, timer-to-task handoff, queued/pending deliveries and locks. Terminal failed work is retained and blocks readiness; recovered pending work cannot be reported idle before native re-claim. Native extraction/retrieval algorithms, retry limits and model parameters are not modified by this patch. Queue recovery remains at-least-once; this is not a claim of exactly-once external model side effects. Corrupt, missing initialized or foreign-deployment state fails closed.

Reproduce: checkout the pinned repository, apply operational.patch from MemoryCore's parent directory, copy MemoryCore/src/core/state/durable-backend.ts and the adapter directories in this bundle, install the repository's declared dependencies, configure separate empty private data and SQLite paths plus Bearer/model secrets, and set TDAI_DURABLE_STATE_DB and TDAI_DURABLE_DEPLOYMENT_ID. Start the native MemoryCore gateway and mm-adapter/mm_adapter.py with adapter/coding-adapter/mm-adapter on PYTHONPATH. Never copy the submitted instance's private state or credentials. The public bundle contains no credentials, evaluation inputs, logs, databases or model responses.

Validation: 17 durable state/filesystem/fault tests (including actual SIGKILL), 8 native factory/manager/worker/status-route integration tests using mock executors, 15 base adapter tests, 6 batch protections and 18 multimodal protections. These are preflight tests, not formal Smoke/Full results or a claim of permanent availability.
