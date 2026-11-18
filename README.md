# TencentDB-NEW · Scoped Recall

An independent retrieval client for [TencentDB Agent Memory][upstream]. It uses the upstream v3 gateway and adds a local evidence-selection layer. This repository does not redistribute or replace MemoryCore.

[中文说明](#中文说明) · [Project page](https://tjusimple.github.io/tencentdb-new/)

## What this client changes

- Queries atomic memory and conversation history concurrently within one explicit team, user and agent scope. It sends at most two search requests and does not write to memory.
- Deduplicates identical evidence across sources, combines the source ranks, and retains the best citation ID from each source. Different case and meaningful whitespace are preserved.
- Picks evidence under a strict UTF-8 byte budget. A large passage that would exceed the remaining budget is skipped so smaller passages may still fit.
- Makes missing sources visible. A partial result is returned only when explicitly requested; an empty result is not used to disguise a failed request.

These are implementation properties, not speed or accuracy claims on a benchmark.

## Try it

Python 3.10+; no third-party packages are required.

```sh
python recall.py --demo
python -m unittest discover -s tests -v
```

To query a TencentDB Agent Memory v3 gateway that you administer, set `TDAI_API_KEY` in your shell. The client defaults to a local gateway, accepts HTTPS for remote gateways, and does not follow redirects. Use your actual service and scope IDs:

```sh
python recall.py --url http://127.0.0.1:8420 --service default --team example-team --user example-user --agent example-agent --budget 4096 "How is the index rebuilt?"
```

Results are written to stdout as JSON. `evidence_bytes` counts only the UTF-8 text payloads, not JSON metadata. The example team, user, agent and demo records are synthetic.

## Version and attribution

The earlier TencentDB-NEW reproduction used TencentDB Agent Memory at [upstream commit `42c6711`][commit] with a separately frozen adaptation. **Scoped Recall is a later implementation: it is not the evaluated snapshot, and no historical leaderboard score is claimed for it.** This project is not an official Tencent Cloud release or endorsement.

TencentDB Agent Memory was created by Tencent and is [MIT-licensed][license]. Upstream remains the source of truth for running the gateway. This repository contains original client code, synthetic fixtures and project documentation. It does not include evaluation data, per-question records, logs, credentials or deployment configuration.

## 中文说明

这是基于腾讯云 TencentDB Agent Memory v3 网关的独立检索客户端。它在同一个团队、用户和 Agent 范围内并行读取两路记忆，合并重复证据、保留来源，再按字节预算输出。可用 `python recall.py --demo` 离线试跑。历史复现使用的上游提交见上文；本仓库不是当时上榜的冻结版本，也不沿用其成绩。

[upstream]: https://github.com/TencentCloud/TencentDB-Agent-Memory
[commit]: https://github.com/TencentCloud/TencentDB-Agent-Memory/tree/42c6711c218b40d38d1d6b0ed4767bcc6069cbfd
[license]: https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/42c6711c218b40d38d1d6b0ed4767bcc6069cbfd/LICENSE
