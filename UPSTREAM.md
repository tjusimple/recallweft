# Compatibility and provenance

RecallWeft is an independent read-only client for the [TencentDB Agent Memory](https://github.com/TencentCloud/TencentDB-Agent-Memory) v3 gateway. TencentDB Agent Memory was created by Tencent and is [MIT-licensed](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/42c6711c218b40d38d1d6b0ed4767bcc6069cbfd/LICENSE). The client does not redistribute MemoryCore. This is not an official Tencent Cloud release.

The earlier TencentDB-NEW reproduction used [upstream commit `42c6711`](https://github.com/TencentCloud/TencentDB-Agent-Memory/tree/42c6711c218b40d38d1d6b0ed4767bcc6069cbfd) with a separately frozen adaptation. RecallWeft is a later implementation, not that evaluated snapshot. It does not claim any historical leaderboard result.

To query a gateway you administer, set the `TDAI_API_KEY` environment variable, then supply your service and scope IDs:

```sh
python recall.py --url http://127.0.0.1:8420 --service default --team example-team --user example-user --agent example-agent "How is the index rebuilt?"
```

The client accepts HTTPS for remote gateways and does not follow redirects. `evidence_bytes` counts the selected UTF-8 text payload, excluding JSON metadata.
