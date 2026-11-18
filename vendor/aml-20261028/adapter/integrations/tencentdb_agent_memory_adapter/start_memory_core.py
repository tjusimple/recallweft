from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path
from typing import Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[2]
EVALUATION_PROFILE = Path(__file__).with_name("tencentdb-evaluation.yaml")
EVALUATION_MODEL = "gpt-4o-mini"


def resolve_memory_core_environment(
    project_root: Path,
    environ: Mapping[str, str] | None = None,
    *,
    model: str = EVALUATION_MODEL,
    gateway_config: Path = EVALUATION_PROFILE,
) -> dict[str, str]:
    """Map LDBD's existing model credentials to MemoryCore variables."""
    resolved = dict(os.environ if environ is None else environ)
    env_path = project_root / ".env.local"
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key:
                resolved.setdefault(key, value.strip())

    api_key = (
        resolved.get("OPENAI_API_KEY")
        or resolved.get("LLM_INTEGRATION_API_KEY")
        or ""
    ).strip()
    base_url = (
        resolved.get("OPENAI_API_BASE")
        or resolved.get("OPENAI_BASE_URL")
        or resolved.get("LLM_INTEGRATION_API_BASE")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError(
            "LDBD model API key is missing; configure OPENAI_API_KEY or "
            "LLM_INTEGRATION_API_KEY in .env.local"
        )
    if not base_url:
        raise RuntimeError(
            "LDBD model API base URL is missing; configure OPENAI_API_BASE, "
            "OPENAI_BASE_URL or LLM_INTEGRATION_API_BASE in .env.local"
        )

    resolved["TDAI_LLM_API_KEY"] = api_key
    resolved["TDAI_LLM_BASE_URL"] = base_url
    resolved["TDAI_LLM_MODEL"] = str(model).strip()
    resolved["TDAI_GATEWAY_CONFIG"] = str(gateway_config.resolve())
    return resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Start TencentDB Agent Memory with LDBD's existing LLM gateway. "
            "Secret values are never printed."
        )
    )
    parser.add_argument(
        "--memory-core-dir",
        required=True,
        type=Path,
        help="Path to the TencentDB-Agent-Memory MemoryCore directory",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
        help="LDBD project root containing .env.local",
    )
    parser.add_argument(
        "--model",
        default=EVALUATION_MODEL,
        help=f"LLM model passed to MemoryCore (default: {EVALUATION_MODEL})",
    )
    parser.add_argument(
        "--gateway-config",
        type=Path,
        default=EVALUATION_PROFILE,
        help="MemoryCore gateway YAML profile",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    memory_core_dir = args.memory_core_dir.resolve()
    gateway_config = args.gateway_config.resolve()
    entrypoint = memory_core_dir / "src" / "gateway" / "server.ts"
    if not entrypoint.is_file():
        raise SystemExit(f"MemoryCore gateway entrypoint not found: {entrypoint}")
    if not gateway_config.is_file():
        raise SystemExit(f"LDBD evaluation profile not found: {gateway_config}")

    try:
        child_env = resolve_memory_core_environment(
            args.project_root.resolve(),
            model=args.model,
            gateway_config=gateway_config,
        )
    except (OSError, RuntimeError) as exc:
        raise SystemExit(str(exc)) from exc

    print(
        "Starting TencentDB Agent Memory with LDBD's existing "
        f"{args.model} configuration."
    )
    completed = subprocess.run(
        ["node", "--import", "tsx", "src/gateway/server.ts"],
        cwd=memory_core_dir,
        env=child_env,
        check=False,
    )
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
