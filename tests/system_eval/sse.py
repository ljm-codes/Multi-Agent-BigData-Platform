"""极简 SSE 解析：服务端每帧只有一行 data:，但按规范做缓冲，避免分块切断。"""
from __future__ import annotations

import json
from typing import Iterator


def iter_sse(response) -> Iterator[tuple[str, dict]]:
    event = "message"
    data_lines: list[str] = []
    for raw in response.iter_lines(chunk_size=1, decode_unicode=True):
        if raw is None:
            continue
        line = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
        if line == "":
            if data_lines:
                payload = "\n".join(data_lines)
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    parsed = {"_raw": payload}
                yield event, parsed
            event = "message"
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].lstrip())
    if data_lines:
        payload = "\n".join(data_lines)
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = {"_raw": payload}
        yield event, parsed
