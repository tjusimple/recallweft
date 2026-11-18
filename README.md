# RecallWeft

A runnable memory service with an evidence-retrieval CLI. RecallWeft searches atomic memories and conversation history, merges exact duplicates, and keeps a citation for each contributing source.

## Run the local service

Requires Python 3.10+ with SQLite FTS5. No cloud account, model key, database server, or pip install is needed. The database starts empty. In one terminal:

```sh
python serve.py start
```

In another terminal in the same directory, write real memories and query them:

```sh
python serve.py remember "The release checklist requires a database snapshot."
python serve.py remember --kind atomic "Database snapshots are retained for seven days."
python serve.py search "database snapshot"
```

Writes are stored in `.recallweft-runtime/local/memory.sqlite3` and survive restart. The service listens only on `127.0.0.1:8420`. It generates a local authorization key in the state directory; the commands above read it automatically. Use `--port` and `--state-dir` before the subcommand if you need a different local instance. `--team`, `--user`, and `--agent` let you select a scoped collection. This is a single-operator local service: the local key grants access to the instance, not just one scope.

The local JSON APIs are `POST /v1/memories/add` (`user_id`, `session_id`, `request_id`, `messages`) and `POST /v1/memories/search` (`user_id`, `query`, optional `top_k` from 1 to 25). Each message has `content` and optional `kind` (`conversation` or `atomic`). Both endpoints accept `team_id` and `agent_id` and require the local key as a Bearer token. A repeated write with the same request ID and content is acknowledged without inserting another copy; a conflicting write returns HTTP 409. `GET /health` checks service readiness.

Run the tests, which start the actual local HTTP service, store and search records, then restart it and read the same data:

```sh
python -m unittest discover -s tests -v
```

## Query an existing gateway

Use a memory gateway that exposes `/v3/atomic/search` and `/v3/conversation/search`. Set `TDAI_API_KEY` in your shell, then supply your gateway URL and the team, user, and agent IDs to search:

```sh
python recall.py --url http://127.0.0.1:8420 --service default --team example-team --user example-user --agent example-agent --limit 8 --budget 4096 "How is the index rebuilt?"
```

Remote gateways must use HTTPS. The command returns JSON with the selected text, citations, and the UTF-8 byte count of the text. If either search fails, the command reports an error by default. Add `--partial` to keep the successful results and list the failed source explicitly.

## AML source and version scope

The local service is an independent SQLite/FTS5 implementation for running RecallWeft without external infrastructure. Atomic memories are written explicitly; the local service does not run automatic model extraction or the full TencentDB engine. It **does not reproduce the ranked AML run**.

[`vendor/aml-20261028/`](vendor/aml-20261028/) contains the previously released text and coding adapter source, operational patch and evaluation configuration, with the upstream license and source notes. That earlier reproduction package uses TencentDB Agent Memory at `42c6711c218b40d38d1d6b0ed4767bcc6069cbfd` as its native backend; it is not the full later ranked runtime. These sources are separate from `serve.py`. Runtime databases, credentials, evaluation inputs, and logs are not included.

[Project page](https://tjusimple.github.io/recallweft/)
