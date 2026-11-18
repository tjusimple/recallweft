# RecallWeft

A read-only CLI for retrieving evidence from atomic memories and conversation history. It searches both in parallel, merges exact duplicates, and keeps a citation for each contributing source. A byte limit keeps the returned text small enough for the caller to use.

## Try it offline

Requires Python 3.10 or later. The demo uses sample records and makes no network calls.

```sh
python recall.py --demo
```

## Query a gateway

Use a memory gateway that exposes `/v3/atomic/search` and `/v3/conversation/search`. Set `TDAI_API_KEY` in your shell, then supply your gateway URL and the team, user, and agent IDs to search:

```sh
python recall.py --url http://127.0.0.1:8420 --service default --team example-team --user example-user --agent example-agent --limit 8 --budget 4096 "How is the index rebuilt?"
```

Remote gateways must use HTTPS. The command returns JSON with the selected text, citations, and the UTF-8 byte count of the text. If either search fails, the command reports an error by default. Add `--partial` to keep the successful results and list the failed source explicitly.

[Project page](https://tjusimple.github.io/recallweft/)
