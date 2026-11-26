#!/usr/bin/env python3
"""Offline checks for copilot_proxy.py: a fake Copilot upstream on 127.0.0.1, no network, no Copilot quota.

    python3 test_offline.py
"""

from __future__ import annotations

import http.client
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

import copilot_proxy as cp

NATIVE, CHAT = "claude-sonnet-5.5", "gpt-test"
STATE: dict[str, Any] = {"models_ok": True, "seen": []}


class Upstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a: Any) -> None:
        pass

    def _json(self, status: int, body: Any) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _sse(self, lines: list[str]) -> None:
        raw = "".join(lines).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        if not STATE["models_ok"]:
            return self._json(503, {"error": "down"})
        self._json(200, {"object": "list", "data": [
            {"id": NATIVE, "capabilities": {"type": "chat", "limits": {"max_output_tokens": 64000}},
             "supported_endpoints": ["/v1/messages", "/chat/completions"]},
            {"id": CHAT, "capabilities": {"type": "chat"}, "supported_endpoints": ["/chat/completions"]}]})

    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        STATE["seen"].append((self.path, body, dict(self.headers.items())))
        if self.path == "/v1/messages":
            if body.get("stream"):
                return self._sse([
                    'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":7}}}\n\n',
                    'event: message_delta\ndata: {"type":"message_delta","usage":{"output_tokens":3}}\n\n',
                    'event: message_stop\ndata: {"type":"message_stop"}\n\n'])
            return self._json(200, {"id": "msg_1", "type": "message", "role": "assistant", "model": NATIVE,
                                    "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn",
                                    "usage": {"input_tokens": 7, "output_tokens": 1}})
        if self.path == "/v1/messages/count_tokens":
            return self._json(200, {"input_tokens": 42})
        if self.path == "/chat/completions":
            if body.get("stream"):
                ch = lambda d, fin=None: "data: " + json.dumps(
                    {"id": "c1", "choices": [{"index": 0, "delta": d, "finish_reason": fin}]}) + "\n\n"
                return self._sse([
                    ch({"content": "hi"}),
                    ch({"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "get_weather",
                                                                                 "arguments": '{"city":'}}]}),
                    ch({"tool_calls": [{"index": 0, "function": {"arguments": '"Paris"}'}}]}),
                    ch({}, "tool_calls"),
                    'data: {"id":"c1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\n',
                    "data: [DONE]\n\n"])
            return self._json(200, {"id": "c1", "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                                    "usage": {"prompt_tokens": 10, "completion_tokens": 1}})
        if self.path == "/embeddings":
            return self._json(200, {"data": []})
        self._json(404, {"error": "unknown endpoint"})


class FakeTokens(cp.TokenManager):
    def __init__(self, base: str):
        super().__init__("gh-test")
        self._base = base

    def get_token(self, force_refresh: bool = False) -> tuple[str, str]:
        return "copilot-test", self._base


def serve(server: Any) -> None:
    threading.Thread(target=server.serve_forever, daemon=True).start()


def start_proxy(base: str, rate: Optional[float] = None, any_model: bool = True) -> Any:
    tm = FakeTokens(base)
    catalog = cp.ModelCatalog(tm, any_model=any_model)
    catalog.get_models_payload(force=True)
    proxy = cp.ProxyServer(("127.0.0.1", 0), tm, catalog, cp.RequestGuard(rate, False, False), False)
    serve(proxy)
    return proxy


def call(proxy: Any, method: str, path: str, body: Any = None,
         headers: Optional[dict[str, str]] = None) -> tuple[int, dict[str, str], str]:
    """(status, headers, body); status 0 = the proxy dropped the connection without an answer."""
    conn = http.client.HTTPConnection("127.0.0.1", proxy.server_address[1], timeout=10)
    try:
        raw = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
        conn.request(method, path, body=raw, headers={"Content-Type": "application/json", **(headers or {})})
        r = conn.getresponse()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read().decode("utf-8", "replace")
    except (http.client.HTTPException, OSError):
        return 0, {}, ""
    finally:
        conn.close()


def main() -> int:
    cp.logging.disable(cp.logging.CRITICAL)
    up = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    serve(up)
    base = f"http://127.0.0.1:{up.server_address[1]}"
    results: list[tuple[str, bool, str]] = []

    def ck(name: str, got: Any, want: Any) -> None:
        results.append((name, got == want, f"got {got!r}, want {want!r}"))

    user = [{"role": "user", "content": "Say ok"}]
    p = start_proxy(base)

    # must pass
    s, _, b = call(p, "GET", "/v1/models")
    ck("P1 models", (s, len(json.loads(b)["data"])), (200, 2))
    s, _, b = call(p, "POST", "/v1/messages", {"model": NATIVE, "max_tokens": 10, "messages": user})
    ck("P2 native plain", (s, STATE["seen"][-1][0]), (200, "/v1/messages"))
    s, _, b = call(p, "POST", "/v1/messages?beta=true", {"model": NATIVE, "max_tokens": 10, "stream": True,
                                                         "messages": user})
    ck("P3 native stream", (s, b.count("event: message_stop")), (200, 1))
    s, _, b = call(p, "POST", "/v1/messages", {"model": CHAT, "max_tokens": 10, "messages": user})
    ck("P4 translated plain", (s, STATE["seen"][-1][0], json.loads(b)["content"][0]["text"]),
       (200, "/chat/completions", "ok"))
    s, _, b = call(p, "POST", "/v1/messages", {"model": CHAT, "max_tokens": 10, "stream": True, "messages": user})
    ck("P5 translated tool stream", (s, '"partial_json":"{\\"city\\":\\"Paris\\"}"' in b,
                                     '"stop_reason":"tool_use"' in b, b.count("event: message_stop")),
       (200, True, True, 1))
    s, _, b = call(p, "POST", "/v1/messages/count_tokens", {"model": NATIVE, "messages": user})
    ck("P6 count_tokens native", (s, json.loads(b)), (200, {"input_tokens": 42}))
    tool_turn = user + [{"role": "assistant", "content": [{"type": "tool_use", "id": "t1", "name": "x", "input": {}}]},
                        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "r"}]}]
    call(p, "POST", "/v1/messages", {"model": NATIVE, "max_tokens": 10, "messages": tool_turn})
    agent = STATE["seen"][-1][2].get("X-Initiator")
    call(p, "POST", "/v1/messages", {"model": NATIVE, "max_tokens": 10,
                                     "messages": tool_turn + [{"role": "assistant", "content": "a"}] + user})
    ck("P7 initiator: tool result = agent, typed follow-up = user",
       (agent, STATE["seen"][-1][2].get("X-Initiator")), ("agent", "user"))
    s, _, b = call(p, "GET", "/stats")
    ck("P8 stats count requests", (s, json.loads(b)["requests"] >= 5), (200, True))
    only = start_proxy(base, any_model=False)
    call(only, "POST", "/v1/messages", {"model": "claude-opus-4-8-20260101", "max_tokens": 10, "messages": user})
    s, _, _ = call(only, "POST", "/v1/chat/completions", {"model": "gpt-4.1", "messages": user})
    ck("P9 default: family mapping, other models refused on OpenAI routes",
       (STATE["seen"][-1][1]["model"], s), ("claude-opus-5.5", 400))

    # must fail cleanly
    ck("F1 bad JSON", call(p, "POST", "/v1/messages", b"{bad")[0], 400)
    ck("F2 messages null", call(p, "POST", "/v1/messages", {"model": NATIVE, "messages": None})[0], 400)
    ck("F3 unknown route", call(p, "POST", "/v1/nope", {})[0], 404)
    ck("F4 /token disabled", call(p, "GET", "/token")[0], 403)
    ck("F5 foreign Origin", call(p, "GET", "/v1/models", headers={"Origin": "https://evil.example"})[0], 403)
    ck("F6 foreign Host", call(p, "GET", "/v1/models", headers={"Host": "evil.example"})[0], 403)
    ck("F7 bad max_tokens, translated route",
       call(p, "POST", "/v1/messages", {"model": CHAT, "max_tokens": "abc", "messages": user})[0], 400)
    ck("F8 non-object message, translated route",
       call(p, "POST", "/v1/messages", {"model": CHAT, "max_tokens": 10, "messages": ["hi"]})[0], 400)
    p.auth_token = "k"
    ck("F9 client key", (call(p, "GET", "/v1/models")[0], call(p, "GET", "/v1/models", headers={"x-api-key": "k"})[0],
                         call(p, "GET", "/health")[0]), (401, 200, 200))
    p.auth_token = None

    # local rate limit: 429 with a Retry-After header, on every upstream route
    r = start_proxy(base, rate=30.0)
    first = call(r, "POST", "/v1/messages", {"model": NATIVE, "max_tokens": 10, "messages": user})[0]
    s, h, _ = call(r, "POST", "/v1/messages", {"model": NATIVE, "max_tokens": 10, "messages": user})
    ck("R1 rate limit 429 + Retry-After", (first, s, h.get("retry-after", "").isdigit()), (200, 429, True))
    ck("R2 rate limit covers /v1/embeddings", call(r, "POST", "/v1/embeddings", {"input": "x"})[0], 429)

    # the model list fails at startup, then Copilot answers again: the native route must come back
    STATE["models_ok"] = False
    late = start_proxy(base, any_model=False)
    STATE["models_ok"] = True
    late.catalog._retry_at = 0.0  # skip the retry pause
    call(late, "POST", "/v1/messages", {"model": NATIVE, "max_tokens": 10, "messages": user})
    ck("C1 catalog reloads after a failed start", STATE["seen"][-1][0], "/v1/messages")

    failed = [r for r in results if not r[1]]
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'} {name}" + ("" if ok else f" ({detail})"))
    print(f"{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
