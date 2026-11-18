from __future__ import annotations

import os

import uvicorn


if __name__ == "__main__":
    uvicorn.run(
        "integrations.tencentdb_agent_memory_adapter.app:create_app",
        host=os.environ.get("LDBD_ADAPTER_HOST", "127.0.0.1"),
        port=int(os.environ.get("LDBD_ADAPTER_PORT", "8430")),
        reload=False,
        factory=True,
    )
