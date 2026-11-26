#!/usr/bin/env python3
"""Offline checks for copilot_proxy.py: a fake Copilot upstream on 127.0.0.1, no network, no login, no Copilot quota.

    python3 test_offline.py              # every check
    python3 test_offline.py stream pool  # only the groups whose name contains one of these words
"""

from __future__ import annotations

import base64
import builtins
import http.client
import json
import os
import socket
import shutil
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import time
from argparse import Namespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Optional

for _k in [k for k in os.environ if k.lower() in ("http_proxy", "https_proxy", "all_proxy")]:
    del os.environ[_k]  # the fake upstream is local: never through a proxy
os.environ["NO_PROXY"] = "127.0.0.1,localhost"

import copilot_proxy as cp  # noqa: E402

NATIVE, CHAT = "claude-sonnet-5.5", "gpt-test"
USER = [{"role": "user", "content": "Say ok"}]
DEAD = "http://127.0.0.1:1"  # nothing listens here: connection refused at once
LOCK = threading.Lock()
MODELS = [
    {"id": NATIVE,
     "capabilities": {"type": "chat", "limits": {"max_output_tokens": 64000, "max_prompt_tokens": 200000}},
     "supported_endpoints": ["/v1/messages", "/chat/completions"]},
    {"id": CHAT, "capabilities": {"type": "chat"}, "supported_endpoints": ["/chat/completions"]}]
# `script`: one entry per upstream POST, consumed in order ("" = answer normally)
STATE: dict[str, Any] = {"models_ok": True, "models": MODELS, "seen": [], "script": [], "conns": 0}

NATIVE_SSE = [
    'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":7,'
    '"cache_read_input_tokens":90}}}\n\n',
    'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta",'
    '"text":"ok"}}\n\n',
    'event: message_delta\ndata: {"type":"message_delta","usage":{"output_tokens":3},"context_management":'
    '{"applied_edits":[{"type":"clear_tool_uses_20250919","cleared_input_tokens":500}]}}\n\n',
    'event: message_stop\ndata: {"type":"message_stop"}\n\n']


def chat_chunk(delta: dict[str, Any], finish: Optional[str] = None) -> str:
    chunk = {"id": "c1", "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    return "data: " + json.dumps(chunk) + "\n\n"


CHAT_SSE = [
    chat_chunk({"content": "hi"}),
    chat_chunk({"tool_calls": [{"index": 0, "id": "call_1",
                                "function": {"name": "get_weather", "arguments": '{"city":'}}]}),
    chat_chunk({"tool_calls": [{"index": 0, "function": {"arguments": '"Paris"}'}}]}),
    chat_chunk({}, "tool_calls"),
    'data: {"id":"c1","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\n',
    "data: [DONE]\n\n"]


class Upstream(BaseHTTPRequestHandler):
    """The fake Copilot API."""
    protocol_version = "HTTP/1.1"

    def setup(self) -> None:
        super().setup()
        with LOCK:
            STATE["conns"] += 1

    def log_message(self, *a: Any) -> None:
        pass

    def _raw(self, status: int, raw: bytes, headers: Optional[dict[str, str]] = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def _json(self, status: int, body: Any, headers: Optional[dict[str, str]] = None) -> None:
        self._raw(status, json.dumps(body).encode(), headers)

    def _sse(self, events: list[str]) -> None:
        """Chunked, one chunk per event, like the real API."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        for e in events:
            raw = e.encode()
            self.wfile.write(b"%x\r\n%s\r\n" % (len(raw), raw))
        self.wfile.write(b"0\r\n\r\n")

    def do_GET(self) -> None:
        if not STATE["models_ok"]:
            return self._json(503, {"error": "down"})
        self._json(200, {"object": "list", "data": STATE["models"]})

    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        with LOCK:
            STATE["seen"].append((self.path, body, dict(self.headers.items())))
            step = STATE["script"].pop(0) if STATE["script"] else ""
        if step == "silent-close":  # answer, then close the connection without announcing it
            self.close_connection = True
        if step == "429":
            return self._json(429, {"error": {"message": "slow down"}}, {"Retry-After": "7", "x-request-id": "req-1"})
        if step == "429-now":
            return self._json(429, {"error": {"message": "slow down"}}, {"Retry-After": "0"})
        if step == "429-long":
            return self._json(429, {"error": {"message": "slow down"}}, {"Retry-After": "100"})
        if step == "401":
            return self._json(401, {"error": "token expired"})
        if step == "404":
            return self._json(404, {"error": "unknown endpoint"})
        if step == "400":
            return self._json(400, {"error": {"message": "prompt is too long"}})
        if step == "unsupported":
            return self._json(400, {"error": {"message": "The requested model is not supported.",
                                              "code": "model_not_supported", "param": "model"}})
        if step == "cm-refused" and "context_management" in body:
            return self._json(400, {"error": {"message": "context_management: Extra inputs are not permitted"}})
        if step == "cm-unsupported" and "context_management" in body:
            return self._json(400, {"error": {"message": "this model does not support context_management"}})
        if step == "not-json":
            return self._raw(200, b"oops")
        if step == "close-header":
            self.close_connection = True
            return self._json(200, {"input_tokens": 42}, {"Connection": "close"})
        if self.path == "/v1/messages":
            if body.get("stream"):
                return self._sse(NATIVE_SSE)
            return self._json(200, {"id": "msg_1", "type": "message", "role": "assistant", "model": NATIVE,
                                    "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn",
                                    "usage": {"input_tokens": 7, "output_tokens": 1}})
        if self.path == "/v1/messages/count_tokens":
            return self._json(200, {"input_tokens": 42})
        if self.path == "/chat/completions":
            if step == "stream-error":
                return self._sse([chat_chunk({"content": "hi"}), 'data: {"error":{"message":"boom"}}\n\n'])
            if step == "stream-cut":
                return self._sse([chat_chunk({"content": "hi"})])
            if step == "surrogate-reply":
                return self._raw(
                    200, b'{"id":"c1","choices":[{"message":{"content":"x\\ud83d"},"finish_reason":"stop"}]}')
            if body.get("stream"):
                return self._sse(CHAT_SSE)
            return self._json(200, {"id": "c1", "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                                    "usage": {"prompt_tokens": 10, "completion_tokens": 1}})
        if self.path == "/responses":
            return self._json(200, {"id": "r1", "output": [], "usage": {"input_tokens": 11, "output_tokens": 2}})
        if self.path == "/embeddings":
            return self._json(200, {"data": []})
        self._json(404, {"error": "unknown endpoint"})


class FakeTokens(cp.TokenManager):
    def __init__(self, base: str):
        super().__init__("gh-test")
        self._base = base
        self.refreshes = 0

    def get_token(self, force_refresh: bool = False) -> tuple[str, str]:
        self.refreshes += bool(force_refresh)
        return "copilot-test", self._base


class BrokenTokens(FakeTokens):
    def get_token(self, force_refresh: bool = False) -> tuple[str, str]:
        raise getattr(cp, "UpstreamError", RuntimeError)("GitHub -> Copilot token exchange failed (500)")


def serve(server: Any) -> Any:
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def start_proxy(base: str, rate: Optional[float] = None, any_model: bool = True, load: bool = True,
                addr: tuple[str, int] = ("127.0.0.1", 0), **kw: Any) -> Any:
    tm = FakeTokens(base)
    catalog = cp.ModelCatalog(tm, any_model=any_model)
    if load:
        catalog.get_models_payload(force=True)
    return serve(cp.ProxyServer(addr, tm, catalog, cp.RequestGuard(rate, False, False), False, **kw))


def call(proxy: Any, method: str, path: str, body: Any = None,
         headers: Optional[dict[str, str]] = None) -> tuple[int, dict[str, str], str]:
    """(status, headers, body); status 0 = the proxy dropped the connection without an answer."""
    host = proxy.server_address[0]
    conn = http.client.HTTPConnection(host, proxy.server_address[1], timeout=10)
    try:
        raw = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
        conn.request(method, path, body=raw, headers={"Content-Type": "application/json", **(headers or {})})
        r = conn.getresponse()
        return r.status, {k.lower(): v for k, v in r.getheaders()}, r.read().decode("utf-8", "replace")
    except (http.client.HTTPException, OSError):
        return 0, {}, ""
    finally:
        conn.close()


def raw_call(proxy: Any, request: bytes) -> int:
    """Send raw bytes; the status of the first answer, 0 when there is none."""
    with socket.create_connection(("127.0.0.1", proxy.server_address[1]), timeout=10) as s:
        s.sendall(request)
        try:
            head = s.recv(4096)
        except OSError:
            return 0
    return int(head.split()[1]) if head.startswith(b"HTTP/") else 0


def raw_exchange(proxy: Any, request: bytes, quiet: float = 1.5) -> bytes:
    """Send raw bytes; everything the proxy answers until it closes the connection or stays quiet."""
    out = b""
    with socket.create_connection(("127.0.0.1", proxy.server_address[1]), timeout=quiet) as s:
        s.sendall(request)
        try:
            while True:
                data = s.recv(65536)
                if not data:
                    break
                out += data
        except OSError:
            pass
    return out


def msg(model: str = NATIVE, **extra: Any) -> dict[str, Any]:
    return {"model": model, "max_tokens": 10, "messages": USER, **extra}


def seen_since(mark: int) -> list[tuple[str, dict[str, Any], dict[str, str]]]:
    return STATE["seen"][mark:]


def tool_history(n: int) -> list[dict[str, Any]]:
    """A conversation with n finished tool calls, ending on a tool result."""
    out: list[dict[str, Any]] = list(USER)
    for i in range(n):
        out.append({"role": "assistant", "content": [{"type": "tool_use", "id": f"t{i}", "name": "x", "input": {}}]})
        out.append({"role": "user", "content": [{"type": "tool_result", "tool_use_id": f"t{i}", "content": "r"}]})
    return out


def events(raw: Any) -> list[tuple[str, dict[str, Any]]]:
    """Anthropic SSE text or byte chunks -> [(event name, data)]."""
    text = raw if isinstance(raw, str) else b"".join(raw).decode()
    out = []
    for block in text.split("\n\n"):
        lines = dict(line.split(": ", 1) for line in block.split("\n") if ": " in line)
        if "data" in lines:
            out.append((lines.get("event", ""), json.loads(lines["data"])))
    return out


def reset_pool() -> None:
    pool = getattr(cp, "UPSTREAM", None)
    if pool is not None:
        pool.clear()
    STATE["conns"] = 0


# ---------------------------------------------------------------------------
# Checks. Each group gets `ck(name, got, want)` and the proxy's upstream base URL.
# ---------------------------------------------------------------------------

Check = Callable[[str, Any, Any], None]
GROUPS: list[Callable[[Check, str], None]] = []


def group(fn: Callable[[Check, str], None]) -> Callable[[Check, str], None]:
    GROUPS.append(fn)
    return fn


@group
def pure_request_translation(ck: Check, base: str) -> None:
    schema = {"type": "object", "properties": {"a": {"type": "integer"}}}
    req = {
        "model": "x", "max_tokens": 9000, "stream": True, "temperature": 0.2, "stop_sequences": ["END"],
        "metadata": {"user_id": "u" * 80},
        "system": [{"type": "text", "text": "S1"}, {"type": "text", "text": "S2"}],
        "messages": [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": [{"type": "thinking", "thinking": "t", "signature": "s"},
                                              {"type": "text", "text": "calling"},
                                              {"type": "tool_use", "id": "tu1", "name": "get", "input": {"a": 1}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1", "is_error": True, "content": [
                    {"type": "text", "text": "boom"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}}]},
                {"type": "text", "text": "next"}]}],
        "tools": [{"name": "get", "description": "d", "input_schema": schema},
                  {"type": "web_search_20250305", "name": "web_search"}],
        "tool_choice": {"type": "any"}}
    out, agent, vision = cp.anthropic_to_openai_request(req, "gpt-test", 4096)
    call_args = out["messages"][2]["tool_calls"][0]["function"].pop("arguments")
    ck("T1 request: whole payload", out, {
        "model": "gpt-test",
        "messages": [
            {"role": "system", "content": "S1\n\nS2"},
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "calling",
             "tool_calls": [{"id": "tu1", "type": "function", "function": {"name": "get"}}]},
            {"role": "tool", "tool_call_id": "tu1", "content": "Error: boom"},
            {"role": "user", "content": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
                                         {"type": "text", "text": "next"}]}],
        "stream": True, "stream_options": {"include_usage": True}, "max_tokens": 4096, "temperature": 0.2,
        "stop": ["END"], "user": "u" * 64,
        "tools": [{"type": "function", "function": {"name": "get", "parameters": schema, "description": "d"}}],
        "tool_choice": "required"})
    ck("T2 request: tool arguments, initiator, vision", (json.loads(call_args), agent, vision), ({"a": 1}, True, True))
    out, agent, vision = cp.anthropic_to_openai_request(
        {"system": "S", "messages": [{"role": "user", "content": [{"type": "text", "text": "a"},
                                                                    {"type": "text", "text": "b"}]},
                                     {"role": "assistant", "content": "x"}, {"role": "user", "content": "y"}],
         "tool_choice": {"type": "tool", "name": "get"}, "tools": [{"name": "get"}]}, "m")
    ck("T3 request: plain text turn", (out["messages"], out["stream"], "max_tokens" in out, out["tool_choice"],
                                       out["tools"][0]["function"]["parameters"], agent, vision),
       ([{"role": "system", "content": "S"}, {"role": "user", "content": "a\nb"},
         {"role": "assistant", "content": "x"}, {"role": "user", "content": "y"}], False, False,
        {"type": "function", "function": {"name": "get"}}, {"type": "object", "properties": {}}, False, False))


@group
def pure_response_translation(ck: Check, base: str) -> None:
    resp = {"id": "c9", "choices": [
        {"message": {"content": "hello", "reasoning_text": "think"}, "finish_reason": "stop"},
        {"message": {"tool_calls": [{"id": "call_1", "function": {"name": "get", "arguments": '{"a":1}'}}]},
         "finish_reason": "tool_calls"}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 5, "prompt_tokens_details": {"cached_tokens": 60}}}
    ck("T4 response: thinking, text, tool call, usage", cp.openai_to_anthropic_response(resp, "m"), {
        "id": "msg_c9", "type": "message", "role": "assistant", "model": "m",
        "content": [{"type": "thinking", "thinking": "think", "signature": ""}, {"type": "text", "text": "hello"},
                    {"type": "tool_use", "id": "call_1", "name": "get", "input": {"a": 1}}],
        "stop_reason": "tool_use", "stop_sequence": None,
        "usage": {"input_tokens": 40, "output_tokens": 5, "cache_read_input_tokens": 60}})
    cut = cp.openai_to_anthropic_response({"choices": [{"message": {"content": "x"}, "finish_reason": "length"}]}, "m")
    empty = cp.openai_to_anthropic_response({"choices": []}, "m")
    bad = cp.openai_to_anthropic_response({"choices": [{"message": {"tool_calls": [
        {"id": "c", "function": {"name": "f", "arguments": "{oops"}}]}}]}, "m")
    ck("T5 response: length, empty, malformed arguments",
       (cut["stop_reason"], empty["content"], empty["stop_reason"], bad["content"][0]["input"]),
       ("max_tokens", [{"type": "text", "text": ""}], "end_turn", {"raw": "{oops"}))


@group
def pure_stream_translation(ck: Check, base: str) -> None:
    tr = cp.StreamTranslator("m")
    out: list[bytes] = []
    def tool(index: int, **parts: Any) -> dict[str, Any]:
        call_id = {"id": parts.pop("id")} if "id" in parts else {}
        return {"choices": [{"delta": {"tool_calls": [{"index": index, **call_id, "function": parts}]}}]}

    for chunk in [
        {"id": "c7", "choices": [{"delta": {"reasoning_text": "r"}}]},
        {"choices": [{"delta": {"content": "a"}}]},
        {"choices": [{"delta": {"content": "b"}}]},
        tool(0, id="call_a", name="f", arguments='{"x"'),
        tool(1, id="call_b", name="g", arguments="{}"),
        tool(0, arguments=":1}"),
        {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]},
        {"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 5}},
    ]:
        out += tr.feed(chunk)
    out += tr.finalize()
    evs = events(out)
    ck("T6 stream: event order", [(e, d.get("index"), (d.get("content_block") or d.get("delta") or {}).get("type"))
                                  for e, d in evs],
       [("message_start", None, None),
        ("content_block_start", 0, "thinking"), ("content_block_delta", 0, "thinking_delta"),
        ("content_block_delta", 0, "signature_delta"), ("content_block_stop", 0, None),
        ("content_block_start", 1, "text"), ("content_block_delta", 1, "text_delta"),
        ("content_block_delta", 1, "text_delta"), ("content_block_stop", 1, None),
        ("content_block_start", 2, "tool_use"), ("content_block_delta", 2, "input_json_delta"),
        ("content_block_stop", 2, None),
        ("content_block_start", 3, "tool_use"), ("content_block_delta", 3, "input_json_delta"),
        ("content_block_stop", 3, None),
        ("message_delta", None, None), ("message_stop", None, None)])
    tools = [(d["content_block"]["id"], d["content_block"]["name"]) for e, d in evs
             if e == "content_block_start" and d["content_block"]["type"] == "tool_use"]
    args = [d["delta"]["partial_json"] for e, d in evs if d.get("delta", {}).get("type") == "input_json_delta"]
    last = [d for e, d in evs if e == "message_delta"][0]
    ck("T7 stream: tool blocks, arguments, stop reason, usage, id",
       (tools, args, last["delta"]["stop_reason"], last["usage"], evs[0][1]["message"]["id"], tr.finalize()),
       ([("call_a", "f"), ("call_b", "g")], ['{"x":1}', "{}"], "tool_use",
        {"input_tokens": 10, "output_tokens": 5}, "msg_c7", []))
    odd = cp.StreamTranslator("m")
    fed = odd.feed({"choices": [{"delta": {"tool_calls": [{"index": None, "function": {"name": "f"}}, "junk"]}}]})
    ck("T8 stream: tool call without an index", (len(fed), len(odd.finalize()) > 0), (1, True))


@group
def pure_models(ck: Check, base: str) -> None:
    STATE["models"] = [{"id": i, "capabilities": {"type": t}} for i, t in (
        ("claude-sonnet-4.6", "chat"), ("claude-sonnet-4", "chat"), ("claude-opus-4.8", "chat"),
        ("claude-haiku-4.5", "chat"), ("gpt-4.1", "chat"), ("text-embedding-3-small", "embeddings"))]
    try:
        cat = cp.ModelCatalog(FakeTokens(base), any_model=True)
        cat.get_models_payload(force=True)
        names = ["claude-sonnet-4-6-20260101", "claude-sonnet-4-6[1m]", "claude-haiku-4-5", "claude-opus-5-5",
                 "claude-3-5-sonnet-latest", "gpt-4.1", "mystery", "text-embedding-3-small", "claude-sonnet-4"]
        ck("T9 models: full catalog mapping", [cat.resolve_model(n) for n in names],
           ["claude-sonnet-4.6", "claude-sonnet-4.6", "claude-haiku-4.5", "claude-opus-4.8", "claude-sonnet-4.6",
            "gpt-4.1", "claude-sonnet-4.6", "claude-sonnet-4.6", "claude-sonnet-4"])
        ck("T10 models: newest per family, chat ids", (cat.newest("opus"), cat.newest("haiku"), cat.chat_model_ids()),
           ("claude-opus-4.8", "claude-haiku-4.5",
            ["claude-haiku-4.5", "claude-opus-4.8", "claude-sonnet-4", "claude-sonnet-4.6", "gpt-4.1"]))
    finally:
        STATE["models"] = MODELS
    only = cp.ModelCatalog(FakeTokens(base))
    only.get_models_payload(force=True)
    ck("T11 models: default mode maps by family",
       [only.resolve_model(n) for n in ("claude-opus-4-8-20260101", "haiku", "gpt-4.1", "")] + [only.missing()],
       ["claude-opus-5.5", "claude-haiku-5.5", "claude-sonnet-5.5", "claude-sonnet-5.5",
        ["claude-opus-5.5", "claude-haiku-5.5"]])
    forced = cp.ModelCatalog(FakeTokens(base), primary="opus")
    try:
        cp.ModelCatalog(FakeTokens(base), primary="gpt-4.1")
        refused = False
    except SystemExit:
        refused = True
    ck("T12 models: -m forces the main model; a model outside the three is refused",
       (forced.resolve_model("claude-sonnet-4-6"), forced.resolve_model("claude-haiku-4-5"), refused),
       ("claude-opus-5.5", "claude-haiku-5.5", True))
    env = cp.claude_code_env("http://127.0.0.1:4141", only, None, None)
    sock = cp.claude_code_env("http://localhost", only, None, None, unix_socket="/tmp/p.sock", auth_token="k")
    ck("T13 Claude Code env", (
        env["ANTHROPIC_MODEL"], env["ANTHROPIC_DEFAULT_OPUS_MODEL"], env["CLAUDE_CODE_SUBAGENT_MODEL"],
        env["ANTHROPIC_AUTH_TOKEN"], env["CLAUDE_CODE_AUTO_MODE_SERVER"], env["CLAUDE_CODE_AUTO_COMPACT_WINDOW"],
        "ANTHROPIC_UNIX_SOCKET" in env, sock["ANTHROPIC_UNIX_SOCKET"], sock["ANTHROPIC_AUTH_TOKEN"]),
       ("claude-sonnet-5.5", "claude-opus-5.5", "claude-haiku-5.5", "dummy", "0", "200000", False, "/tmp/p.sock", "k"))


@group
def pure_context_edit(ck: Check, base: str) -> None:
    saved = cp.CLEAR_TRIGGER, cp.CLEAR_KEEP, cp.CLEAR_STEP
    cp.CLEAR_TRIGGER, cp.CLEAR_KEEP, cp.CLEAR_STEP = 100000, 3, 10
    try:
        def keep(n: int, mode: str = "tools", **extra: Any) -> Any:
            cm = cp.context_management_for({"messages": tool_history(n), **extra}, mode)
            return cm and [e["keep"]["value"] for e in cm["edits"]]
        ck("T14 context edit: kept tool results per step", [keep(n) for n in (3, 5, 13, 19, 23)] + [keep(13, "off")],
           [None, [3], [3], [9], [3], None])
        think = {"thinking": {"type": "enabled", "budget_tokens": 2000}}
        own_tools = {"context_management": {"edits": [{"type": "clear_tool_uses_20250919"}]}}
        own_think = {"context_management": {"edits": [{"type": "clear_thinking_20251015", "keep": "all"}], "x": 1}}
        both = cp.context_management_for({"messages": tool_history(13), **think, **own_think}, "both")
        ck("T15 context edit: the client's own edits are kept; thinking first", (
            cp.context_management_for({"messages": tool_history(13), **own_tools}, "tools"),
            both and ([e["type"] for e in both["edits"]], both["edits"][0]["keep"], both["x"]),
            keep(2, "thinking", **think), keep(2, "thinking"),
            cp.context_management_for({"messages": tool_history(13), "context_management": "x"}, "tools")),
           (None, (["clear_thinking_20251015", "clear_tool_uses_20250919"], "all", 1), [1], None, None))
    finally:
        cp.CLEAR_TRIGGER, cp.CLEAR_KEEP, cp.CLEAR_STEP = saved
    ev = {"type": "message_delta", "delta": {"context_management": {"applied_edits": [
        {"cleared_input_tokens": 300}, {"cleared_input_tokens": 200}, {"other": 1}]}}}
    ck("T16 cleared tokens", (cp.cleared_input_tokens(ev), cp.cleared_input_tokens({"type": "x"}),
                              cp.cleared_input_tokens("junk")), (500, None, None))


@group
def pure_helpers(ck: Check, base: str) -> None:
    ck("T17 retry wait", [cp._retry_after(h, a) for h, a in (
        ("5", 0), ("100", 0), ("Wed, 21 Oct 2026 07:28:00 GMT", 0), (None, 0), (None, 2), (None, 10), ("nan", 0),
        ("-1", 0))], [5.0, None, None, 1.5, 6.0, 20.0, None, None])
    ck("T18 host names", [cp.host_name(v) for v in (
        "localhost:4141", "LOCALHOST", "[::1]:80", "::1", "example.com.", "a b", "", "host:abc")],
       ["localhost", "localhost", "[::1]", "[::1]", "example.com", None, None, None])
    ck("T19 native route refused or not", [cp._route_unsupported(s, b) for s, b in (
        (404, b""), (400, b'{"error":{"code":"model_not_supported"}}'), (400, b"The requested model is not supported"),
        (400, b"context_management: Extra inputs are not permitted"), (400, b"prompt is too long"),
        (400, b"thinking is not supported for this model"), (429, b""), (500, b"model not supported"))],
       [True, True, True, False, False, False, False, False])
    text = {"messages": [{"role": "user", "content": "x" * 370}]}
    image = {"messages": [{"role": "user", "content": [{"type": "image", "source": {"data": "A" * 9000}}]}]}
    ck("T20 token estimate", (100 < cp.estimate_tokens(text) < 120, 1200 < cp.estimate_tokens(image) < 1220,
                              cp.estimate_tokens({"messages": []})), (True, True, 1))
    typed = tool_history(1) + [{"role": "assistant", "content": "a"}] + USER
    pic = [{"role": "user", "content": [{"type": "image", "source": {}}]}]
    ck("T21 initiator and vision flags", (
        cp.anthropic_flags({"messages": tool_history(1)}), cp.anthropic_flags({"messages": typed}),
        cp.anthropic_flags({"messages": pic}),
        cp.openai_flags({"messages": [{"role": "user", "content": "q"}, {"role": "tool", "content": "r"}]}),
        cp.openai_flags({"input": [{"type": "function_call_output", "output": "r"}]}),
        cp.openai_flags({"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {}}]}]}),
        cp.openai_flags({"input": [{"role": "user", "content": [{"type": "input_image"}]}]}),
        cp.openai_flags({"input": "plain text"})),
       ((True, False), (False, False), (False, True), (True, False), (True, False), (False, True), (False, True),
        (False, False)))
    with tempfile.TemporaryDirectory() as tmp:
        log = cp.UsageLog(os.path.join(tmp, "sub", "usage.jsonl"))
        log.record(route="native", model="m", status=200, cleared_input_tokens=50,
                   usage={"input_tokens": 10, "cache_read_input_tokens": 90, "output_tokens": 5})
        log.record(route="translate", model="m", status=429, usage={"input_tokens": "junk"})
        snap = log.snapshot()
        rows = [json.loads(line) for line in Path(tmp, "sub", "usage.jsonl").read_text().splitlines()]
        ck("T22 usage log", (snap["requests"], snap["errors"], snap["input_tokens"], snap["cache_hit_ratio"],
                             snap["cleared_input_tokens"], snap["by_route"]["native"]["requests"],
                             snap["by_model"]["m"]["requests"], len(rows), rows[1]["usage"]["input_tokens"]),
           (2, 1, 10, 0.9, 50, 1, 2, 2, 0))
        token = Path(tmp, "login", "github_token")
        cp.save_github_token(token, "  ghu_abc \n")
        ck("T23 token file: 0600, 0700 directory, stripped", (
            cp.load_github_token(token), stat.S_IMODE(token.stat().st_mode), stat.S_IMODE(token.parent.stat().st_mode),
            cp.load_github_token(Path(tmp, "none"))), ("ghu_abc", 0o600, 0o700, None))
        args = Namespace(token_file=str(token), copilot_config_dir=os.path.join(tmp, "cfg"), no_copilot_config=False)
        sb = cp.sandbox_settings(args)
        ck("T24 sandbox settings", (sb["sandbox"]["enabled"], sb["sandbox"]["allowUnsandboxedCommands"],
                                    str(token) in sb["sandbox"]["filesystem"]["denyRead"],
                                    f"Read(/{token})" in sb["permissions"]["deny"]), (True, False, True, True))
    saved = dict(os.environ)
    try:
        os.environ.update(HTTPS_PROXY="http://u:p%40ss@proxy.local:3128", NO_PROXY="skip.example")
        via = cp.upstream_proxy("https", "api.example")
        skipped = cp.upstream_proxy("https", "skip.example")
        os.environ["HTTPS_PROXY"] = "socks5://proxy.local:1080"
        try:
            cp.upstream_proxy("https", "api.example")
            closed = False
        except OSError:
            closed = True
        ck("T25 HTTPS_PROXY: host, port, credentials; NO_PROXY; socks refused", (via, skipped, closed), (
            ("proxy.local", 3128, {"Proxy-Authorization": "Basic " + base64.b64encode(b"u:p@ss").decode()}),
            None, True))
    finally:
        os.environ.clear()
        os.environ.update(saved)


@group
def routes(ck: Check, base: str) -> None:
    p = start_proxy(base)
    s, _, b = call(p, "GET", "/v1/models")
    ck("P1 models", (s, len(json.loads(b)["data"])), (200, 2))
    s, _, b = call(p, "POST", "/v1/messages", msg())
    ck("P2 native plain", (s, STATE["seen"][-1][0], json.loads(b)["content"][0]["text"]), (200, "/v1/messages", "ok"))
    s, h, b = call(p, "POST", "/v1/messages?beta=true", msg(stream=True))
    ck("P3 native stream: forwarded byte for byte", (s, b, h.get("content-type", "").split(";")[0]),
       (200, "".join(NATIVE_SSE), "text/event-stream"))
    s, _, b = call(p, "POST", "/v1/messages", msg(CHAT))
    ck("P4 translated plain", (s, STATE["seen"][-1][0], json.loads(b)["content"][0]["text"], json.loads(b)["usage"]),
       (200, "/chat/completions", "ok", {"input_tokens": 10, "output_tokens": 1}))
    s, _, b = call(p, "POST", "/v1/messages", msg(CHAT, stream=True))
    evs = events(b)
    ck("P5 translated tool stream", (
        s, [d["delta"]["partial_json"] for e, d in evs if d.get("delta", {}).get("type") == "input_json_delta"],
        [d["delta"]["stop_reason"] for e, d in evs if e == "message_delta"], [e for e, _ in evs][-1],
        "".join(d["delta"]["text"] for e, d in evs if d.get("delta", {}).get("type") == "text_delta")),
       (200, ['{"city":"Paris"}'], ["tool_use"], "message_stop", "hi"))
    s, _, b = call(p, "POST", "/v1/messages/count_tokens", {"model": NATIVE, "messages": USER})
    est = call(p, "POST", "/v1/messages/count_tokens", {"model": CHAT, "messages": USER})
    ck("P6 count_tokens: exact for Claude, estimate otherwise",
       (s, json.loads(b), est[0], json.loads(est[2])["input_tokens"] > 0), (200, {"input_tokens": 42}, 200, True))
    call(p, "POST", "/v1/messages", msg(messages=tool_history(1)))
    agent = STATE["seen"][-1][2].get("X-Initiator")
    call(p, "POST", "/v1/messages", msg(messages=tool_history(1) + [{"role": "assistant", "content": "a"}] + USER))
    ck("P7 initiator: tool result = agent, typed follow-up = user",
       (agent, STATE["seen"][-1][2].get("X-Initiator")), ("agent", "user"))
    only = start_proxy(base, any_model=False)
    call(only, "POST", "/v1/messages", msg("claude-opus-4-8-20260101"))
    s, _, _ = call(only, "POST", "/v1/chat/completions", {"model": "gpt-4.1", "messages": USER})
    ck("P9 default: family mapping, other models refused on OpenAI routes",
       (STATE["seen"][-1][1]["model"], s, len(json.loads(call(only, "GET", "/v1/models")[2])["data"])),
       ("claude-opus-5.5", 400, 3))
    s, _, b = call(p, "POST", "/v1/chat/completions", {"model": CHAT, "messages": USER})
    st, _, sb = call(p, "POST", "/v1/chat/completions", {"model": CHAT, "messages": USER, "stream": True})
    ck("P10 OpenAI chat: plain and stream passed through",
       (s, json.loads(b)["choices"][0]["message"]["content"], st, sb), (200, "ok", 200, "".join(CHAT_SSE)))
    s, _, b = call(p, "POST", "/v1/responses", {"model": CHAT, "input": "hi"})
    e = call(p, "POST", "/v1/embeddings", {"model": "text-embedding-3-small", "input": "x"})
    ck("P11 OpenAI responses and embeddings", (s, json.loads(b)["id"], e[0], STATE["seen"][-1][0]),
       (200, "r1", 200, "/embeddings"))
    ck("P12 health, HEAD, OPTIONS, unknown GET", (
        json.loads(call(p, "GET", "/health")[2])["status"], call(p, "HEAD", "/")[0],
        call(p, "OPTIONS", "/v1/messages")[0], call(p, "GET", "/nope")[0]), ("ok", 200, 204, 404))
    big = msg(max_tokens=100000, thinking={"type": "enabled", "budget_tokens": 80000})
    call(p, "POST", "/v1/messages", big)
    sent = STATE["seen"][-1][1]
    ck("P13 native: max_tokens and thinking budget kept inside the model limit",
       (sent["max_tokens"], sent["thinking"]["budget_tokens"], sent["model"]), (64000, 62976, NATIVE))
    hdr = {"anthropic-beta": "a-1, b-2", "anthropic-version": "2023-06-01", "x-secret": "no"}
    call(p, "POST", "/v1/messages", msg(), headers=hdr)
    up = {k.lower(): v for k, v in STATE["seen"][-1][2].items()}
    ck("P14 native: Anthropic headers forwarded, others not; Copilot identity added", (
        [x.strip() for x in up.get("anthropic-beta", "").split(",")], up.get("anthropic-version"), "x-secret" in up,
        up.get("authorization"), up.get("copilot-integration-id")),
       (["a-1", "b-2"], "2023-06-01", False, "Bearer copilot-test", "vscode-chat"))


@group
def stats(ck: Check, base: str) -> None:
    p = start_proxy(base)
    call(p, "POST", "/v1/messages", msg(stream=True))
    call(p, "POST", "/v1/messages", msg(CHAT, stream=True))
    call(p, "POST", "/v1/responses", {"model": CHAT, "input": "hi"})
    STATE["script"] = ["400"]
    call(p, "POST", "/v1/messages", msg())
    s, _, b = call(p, "GET", "/stats")
    t = json.loads(b)
    ck("P8 stats: tokens read from every route's answer", (
        s, t["requests"], t["errors"], t["input_tokens"], t["cache_read_input_tokens"], t["output_tokens"],
        t["cleared_input_tokens"], sorted(t["by_route"]), t["by_route"]["responses"]["input_tokens"]),
       (200, 4, 1, 7 + 10 + 11, 90, 3 + 5 + 2, 500, ["native", "responses", "translate"], 11))


@group
def bad_requests(ck: Check, base: str) -> None:
    p = start_proxy(base)
    ck("F1 bad JSON", call(p, "POST", "/v1/messages", b"{bad")[0], 400)
    ck("F2 messages null", call(p, "POST", "/v1/messages", {"model": NATIVE, "messages": None})[0], 400)
    ck("F3 unknown route", call(p, "POST", "/v1/nope", {})[0], 404)
    ck("F4 /token disabled", call(p, "GET", "/token")[0], 403)
    ck("F5 foreign Origin", call(p, "GET", "/v1/models", headers={"Origin": "https://evil.example"})[0], 403)
    ck("F6 foreign Host", call(p, "GET", "/v1/models", headers={"Host": "evil.example"})[0], 403)
    ck("F7 bad max_tokens, translated route", call(p, "POST", "/v1/messages", msg(CHAT, max_tokens="abc"))[0], 400)
    ck("F8 non-object message, translated route", call(p, "POST", "/v1/messages", msg(CHAT, messages=["hi"]))[0], 400)
    p.auth_token = "k"
    ck("F9 client key", (call(p, "GET", "/v1/models")[0], call(p, "GET", "/v1/models", headers={"x-api-key": "k"})[0],
                         call(p, "GET", "/v1/models", headers={"Authorization": "Bearer k"})[0],
                         call(p, "GET", "/v1/models", headers={"Authorization": "Bearer wrong"})[0],
                         call(p, "GET", "/health")[0]), (401, 200, 200, 401, 200))
    p.auth_token = None
    inf = b'{"model":"%s","max_tokens":1e999,"messages":[{"role":"user","content":"x"}]}'
    ck("F10 max_tokens out of range, both routes",
       [call(p, "POST", "/v1/messages", inf % m.encode())[0] for m in (CHAT, NATIVE)], [400, 400])
    ck("F11 JSON nested 100,000 deep", call(p, "POST", "/v1/messages", b"[" * 100000)[0], 400)
    ck("F12 count_tokens without a messages list",
       (call(p, "POST", "/v1/messages/count_tokens", {"model": CHAT, "messages": None})[0],
        call(p, "POST", "/v1/messages/count_tokens", {"model": CHAT, "messages": 5})[0]), (400, 400))
    ck("F13 body is not an object", call(p, "POST", "/v1/messages", b"[1,2]")[0], 400)
    ok = b'{"model":"%s","max_tokens":10,"messages":[{"role":"user","content":"x"}]}' % NATIVE.encode()
    head = b"POST /v1/messages HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n"
    chunked = head + b"Transfer-Encoding: chunked\r\n\r\n" + b"%x\r\n%s\r\n0\r\n\r\n" % (len(ok), ok)
    ck("F14 request framing: chunked body, bad chunk size, two lengths, unknown coding", (
        raw_call(p, chunked),
        raw_call(p, head + b"Transfer-Encoding: chunked\r\n\r\nzz\r\n"),
        raw_call(p, head + b"Content-Length: 5\r\nContent-Length: 6\r\n\r\nabcdef"),
        raw_call(p, head + b"Transfer-Encoding: gzip\r\n\r\n")), (200, 400, 400, 400))
    saved, cp.MAX_BODY = cp.MAX_BODY, 100
    try:
        ck("F15 body over the size limit", call(p, "POST", "/v1/messages", msg(messages=USER * 20))[0], 413)
    finally:
        cp.MAX_BODY = saved
    lone = b'{"model":"%s","max_tokens":10,"messages":[{"role":"user","content":"x\\ud83d"}]}'
    native = call(p, "POST", "/v1/messages", lone % NATIVE.encode())[0]
    got_native = STATE["seen"][-1][1]["messages"][0]["content"]
    translated = call(p, "POST", "/v1/messages", lone % CHAT.encode())[0]
    STATE["script"] = ["surrogate-reply"]
    reply = call(p, "POST", "/v1/messages", msg(CHAT))[0]
    ck("F16 half an emoji in the text (lone surrogate) passes both ways",
       (native, got_native, translated, STATE["seen"][-2][1]["messages"][0]["content"], reply),
       (200, "x\ud83d", 200, "x\ud83d", 200))
    cors = start_proxy(base)
    cors.cors_origins = {"https://app.example"}
    s, h, _ = call(cors, "GET", "/v1/models", headers={"Origin": "https://app.example"})
    ck("F17 allowed Origin gets the CORS header; another one is still refused", (
        s, h.get("access-control-allow-origin"),
        call(cors, "GET", "/v1/models", headers={"Origin": "https://evil.example"})[0]),
       (200, "https://app.example", 403))


@group
def upstream_errors(ck: Check, base: str) -> None:
    p = start_proxy(base)
    STATE["script"] = ["429"]
    s, h, b = call(p, "POST", "/v1/messages", msg())
    ck("E1 native 429: status, Retry-After and body passed on", (s, h.get("retry-after"), h.get("x-request-id"),
                                                                "slow down" in b), (429, "7", "req-1", True))
    STATE["script"] = ["429"]
    s, h, b = call(p, "POST", "/v1/messages", msg(CHAT))
    ck("E2 translated 429: Anthropic error shape with Retry-After",
       (s, h.get("retry-after"), json.loads(b)["error"]["type"]), (429, "7", "rate_limit_error"))
    STATE["script"], mark = ["401"], len(STATE["seen"])
    s, _, _ = call(p, "POST", "/v1/messages", msg())
    ck("E3 Copilot 401: one token refresh, then the answer", (s, len(seen_since(mark)), p.tm.refreshes), (200, 2, 1))
    STATE["script"], mark = ["404"], len(STATE["seen"])
    s, _, b = call(p, "POST", "/v1/messages", msg(stream=True))
    ck("E4 native route refused: falls back to the translated route",
       (s, [x[0] for x in seen_since(mark)], events(b)[-1][0]),
       (200, ["/v1/messages", "/chat/completions"], "message_stop"))
    STATE["script"], mark = ["400"], len(STATE["seen"])
    s, _, b = call(p, "POST", "/v1/messages", msg())
    ck("E5 a real 400 goes back unchanged, no second request", (s, "prompt is too long" in b, len(seen_since(mark))),
       (400, True, 1))
    STATE["script"] = ["not-json"]
    ck("E6 unreadable 200 from Copilot", call(p, "POST", "/v1/messages", msg(CHAT))[0], 502)
    STATE["script"] = ["stream-error"]
    s, _, b = call(p, "POST", "/v1/messages", msg(CHAT, stream=True))
    ck("E7 error inside a translated stream becomes an error event",
       (s, "event: error" in b, "boom" in b, "event: message_stop" in b), (200, True, True, False))
    STATE["script"] = ["stream-cut"]
    s, _, b = call(p, "POST", "/v1/messages", msg(CHAT, stream=True))
    ck("E8 a translated stream that stops early is not reported as complete",
       (s, "event: error" in b, "event: message_stop" in b), (200, True, False))
    saved, cp.RETRIES = cp.RETRIES, 1
    try:
        STATE["script"], mark = ["429-now"], len(STATE["seen"])
        first = (call(p, "POST", "/v1/messages", msg())[0], len(seen_since(mark)))
        STATE["script"], mark = ["429-long"], len(STATE["seen"])
        ck("E9 COPILOT_PROXY_RETRIES=1: retried on a short wait, not on a long one",
           (first, call(p, "POST", "/v1/messages", msg())[0], len(seen_since(mark))), ((200, 2), 429, 1))
    finally:
        cp.RETRIES = saved
    broken = start_proxy(base)
    broken.tm = BrokenTokens(base)
    chat = {"model": CHAT, "messages": USER}
    ck("E10 the token exchange fails",
       (call(broken, "POST", "/v1/messages", msg())[0], call(broken, "POST", "/v1/chat/completions", chat)[0]),
       (502, 502))


@group
def upstream_unreachable(ck: Check, base: str) -> None:
    only = start_proxy(DEAD, any_model=False, load=False)
    ck("U1 default mode, Copilot unreachable: model list still served, messages 502, count estimated", (
        call(only, "GET", "/v1/models")[0], call(only, "POST", "/v1/messages", msg())[0],
        call(only, "POST", "/v1/messages/count_tokens", {"model": NATIVE, "messages": USER})[0]), (200, 502, 200))
    full = start_proxy(DEAD, any_model=True, load=False)
    ck("U2 --any-model, Copilot unreachable", (call(full, "GET", "/v1/models")[0],
                                                call(full, "POST", "/v1/messages", msg())[0]), (502, 502))
    saved, cp.GITHUB_API_BASE_URL = cp.GITHUB_API_BASE_URL, DEAD
    try:
        ck("U3 /usage, GitHub unreachable", call(only, "GET", "/usage")[0], 502)
    finally:
        cp.GITHUB_API_BASE_URL = saved
    STATE["models_ok"] = False
    late = start_proxy(base, any_model=False)
    STATE["models_ok"] = True
    late.catalog._retry_at = 0.0  # skip the one-minute pause between attempts
    call(late, "POST", "/v1/messages", msg())
    ck("C1 the model list failed at start: the native route comes back", STATE["seen"][-1][0], "/v1/messages")


@group
def context_edit(ck: Check, base: str) -> None:
    p = start_proxy(base)
    p.context_edit = "tools"
    call(p, "POST", "/v1/messages", msg(messages=tool_history(13)), headers={"anthropic-beta": "a-1"})
    _, sent, hdr = STATE["seen"][-1]
    call(p, "POST", "/v1/messages", msg(messages=tool_history(2)))
    ck("X1 context edit added with its beta; not below the keep count", (
        [e["keep"]["value"] for e in sent["context_management"]["edits"]],
        {k.lower(): v for k, v in hdr.items()}.get("anthropic-beta"), "context_management" in STATE["seen"][-1][1]),
       ([3], "a-1,context-management-2025-06-27", False))
    STATE["script"], mark = ["cm-refused"], len(STATE["seen"])
    s, _, _ = call(p, "POST", "/v1/messages", msg(messages=tool_history(13)))
    again = seen_since(mark)
    call(p, "POST", "/v1/messages", msg(messages=tool_history(13)))
    ck("X2 Copilot refuses the added edit: sent again as the client wrote it; still tried next time", (
        s, ["context_management" in x[1] for x in again], "context_management" in STATE["seen"][-1][1]),
       (200, [True, False], True))
    STATE["script"] = ["cm-unsupported"]
    s, _, _ = call(p, "POST", "/v1/messages", msg(messages=tool_history(13)))
    call(p, "POST", "/v1/messages", msg(messages=tool_history(13)))
    ck("X3 the model does not support it: not added again for that model",
       (s, "context_management" in STATE["seen"][-1][1]), (200, False))


@group
def model_fit(ck: Check, base: str) -> None:
    native = {"supported_endpoints": ["/v1/messages"]}
    STATE["models"] = [
        {"id": "claude-haiku-4.5", **native, "capabilities": {
            "type": "chat", "limits": {"max_output_tokens": 32000},
            "supports": {"max_thinking_budget": 32000, "min_thinking_budget": 1024, "streaming": True}}},
        {"id": "claude-sonnet-4.6", **native, "capabilities": {"type": "chat", "supports": {
            "adaptive_thinking": True, "max_thinking_budget": 32000,
            "reasoning_effort": ["low", "medium", "high", "max"]}}},
        {"id": "claude-opus-4.8", "capabilities": {"type": "chat"}, **native}]
    try:
        p = start_proxy(base)
    finally:
        STATE["models"] = MODELS
    haiku, sonnet, opus = "claude-haiku-4-5", "claude-sonnet-4-6", "claude-opus-4-8"

    def sent(model: str, **fields: Any) -> dict[str, Any]:
        """What Copilot receives for this request."""
        headers = {"anthropic-beta": "a-1,per-turn-control-2026-07-01,effort-2025-11-24"}
        status = call(p, "POST", "/v1/messages", msg(model, **fields), headers=headers)[0]
        path, body, hdr = STATE["seen"][-1]
        return {**body, "status": status, "betas": {k.lower(): v for k, v in hdr.items()}.get("anthropic-beta"),
                "initiator": hdr.get("X-Initiator")}

    fmt = {"format": {"type": "json_schema"}}
    a, b = sent(haiku, output_config={"effort": "medium"}), sent(haiku, output_config={"effort": "high", **fmt})
    ck("X4 effort: removed for a model without reasoning effort, other output settings kept",
       (a["status"], "output_config" in a, b["output_config"]), (200, False, fmt))
    ck("X5 effort: lowered to the nearest level the model has; a listed level is not touched",
       [sent(sonnet, output_config={"effort": e})["output_config"] for e in ("xhigh", "medium", "max")],
       [{"effort": "high"}, {"effort": "medium"}, {"effort": "max"}])
    ck("X6 effort: unchanged when Copilot lists no capabilities for the model, or the value is not a known level",
       (sent(opus, output_config={"effort": "xhigh"})["output_config"],
        sent(sonnet, output_config={"effort": "turbo"})["output_config"]), ({"effort": "xhigh"}, {"effort": "turbo"}))
    low = {"max_tokens": 9, "messages": USER, "output_config": {"effort": "low"}}
    if hasattr(cp, "fit_to_model"):
        cp.fit_to_model(low, {"reasoning_effort": ["high", "max"]})
    ck("X7 effort: below every level the model has -> its lowest", low["output_config"], {"effort": "high"})
    adaptive = {"type": "adaptive", "display": "omitted"}
    a = sent(haiku, max_tokens=128000, thinking=adaptive)
    ck("X8 adaptive thinking: a fixed budget on a model without it; untouched on a model that has it",
       (a["thinking"], a["max_tokens"], sent(haiku, max_tokens=1500, thinking=adaptive).get("thinking"),
        sent(sonnet, max_tokens=128000, thinking=adaptive)["thinking"]),
       ({"type": "enabled", "display": "omitted", "budget_tokens": 30976}, 32000, None, adaptive))

    def per_turn(effort: str) -> list[dict[str, Any]]:
        return tool_history(1) + [{"role": "system", "content": [], "output_config": {"effort": effort}}]

    a, b, c = sent(haiku, messages=per_turn("medium")), sent(sonnet, messages=per_turn("medium")), \
        sent(sonnet, messages=per_turn("xhigh"))
    ck("X9 per-turn effort message: removed with its beta for a model without effort; kept or lowered otherwise",
       (a["messages"] == tool_history(1), a["betas"], b["messages"] == per_turn("medium"), b["betas"],
        c["messages"] == per_turn("high")),
       (True, "a-1,effort-2025-11-24", True, "a-1,per-turn-control-2026-07-01,effort-2025-11-24", True))
    # the order Claude Code 2.1.332 sends (measured): prompt, per-turn message, tool call, tool result
    real = USER + per_turn("medium")[-1:] + tool_history(1)[1:]
    ck("X10 a tool follow-up stays agent-initiated wherever the per-turn message sits (after the prompt, or last)",
       (sent(sonnet, messages=real)["initiator"], a["initiator"], b["initiator"],
        cp.anthropic_flags({"messages": per_turn("medium")}),
        cp.anthropic_flags({"messages": USER + per_turn("x")[-1:]})),
       ("agent", "agent", "agent", (True, False), (False, False)))


@group
def default_mode_55(ck: Check, base: str) -> None:
    """The default mode end to end: a Copilot account that serves the three Claude 5.5 models. Their capabilities
    here are the ones Copilot listed for the 4.x models on 2026-11-26 (no account with 5.5 was available)."""
    def model(mid: str, out: int, **supports: Any) -> dict[str, Any]:
        return {"id": mid, "supported_endpoints": ["/v1/messages", "/chat/completions"], "capabilities": {
            "type": "chat", "limits": {"max_output_tokens": out, "max_prompt_tokens": 128000},
            "supports": {"max_thinking_budget": 32000, "min_thinking_budget": 1024, **supports}}}

    STATE["models"] = [
        model("claude-sonnet-5.5", 64000, adaptive_thinking=True, reasoning_effort=["low", "medium", "high", "max"]),
        model("claude-opus-5.5", 64000, adaptive_thinking=True,
              reasoning_effort=["low", "medium", "high", "xhigh", "max"]),
        model("claude-haiku-5.5", 32000)]
    try:
        p = start_proxy(base, any_model=False)
    finally:
        STATE["models"] = MODELS
    s, _, b = call(p, "GET", "/v1/models")
    listed = json.loads(b)["data"]
    ck("D1 only the three 5.5 models are listed, with Copilot's own entries; none is missing",
       (s, [m["id"] for m in listed], all("capabilities" in m for m in listed), p.catalog.missing()),
       (200, ["claude-sonnet-5.5", "claude-opus-5.5", "claude-haiku-5.5"], True, []))
    asked = ["claude-fable-5-2", "claude-sonnet-4-6", "claude-opus-4-8", "claude-haiku-4-5-20251001", "gpt-4.1"]
    sent = []
    for name in asked:
        status = call(p, "POST", "/v1/messages", msg(name))[0]
        sent.append((status, STATE["seen"][-1][0], STATE["seen"][-1][1]["model"]))
    ck("D2 every name goes to the 5.5 model of its family, on the native route", sent, [
        (200, "/v1/messages", m) for m in ("claude-sonnet-5.5", "claude-sonnet-5.5", "claude-opus-5.5",
                                           "claude-haiku-5.5", "claude-sonnet-5.5")])
    # what Claude Code 2.1.332 sends when it believes the model is claude-fable-5-2
    per_turn = {"role": "system", "content": [], "output_config": {"effort": "medium"}}
    code = {"max_tokens": 128000, "thinking": {"type": "adaptive", "display": "omitted"},
            "output_config": {"effort": "medium"}, "messages": tool_history(1) + [per_turn]}
    betas = {"anthropic-beta": "claude-code-20250219,per-turn-control-2026-07-01,effort-2025-11-24"}
    got = {}
    for name in ("claude-sonnet-4-6", "claude-opus-4-8", "claude-haiku-4-5"):
        status = call(p, "POST", "/v1/messages", msg(name, **code), headers=betas)[0]
        _, body, hdr = STATE["seen"][-1]
        sent_betas = {k.lower(): v for k, v in hdr.items()}.get("anthropic-beta", "")
        got[body["model"]] = (status, body["max_tokens"], body["thinking"], body.get("output_config"),
                              len(body["messages"]), "per-turn-control-2026-07-01" in sent_betas,
                              hdr.get("X-Initiator"))
    kept = (200, 64000, code["thinking"], {"effort": "medium"}, 4, True, "agent")
    ck("D3 Claude Code's settings: kept on Sonnet and Opus 5.5; fitted on Haiku 5.5; tool follow-up stays agent", got, {
        "claude-sonnet-5.5": kept, "claude-opus-5.5": kept,
        "claude-haiku-5.5": (200, 32000, {"type": "enabled", "display": "omitted", "budget_tokens": 30976}, None, 3,
                             False, "agent")})
    efforts = {}
    for name in ("claude-sonnet-4-6", "claude-opus-4-8"):
        call(p, "POST", "/v1/messages", msg(name, output_config={"effort": "xhigh"}))
        efforts[STATE["seen"][-1][1]["model"]] = STATE["seen"][-1][1]["output_config"]["effort"]
    ck("D4 effort xhigh: lowered to high on Sonnet 5.5, kept on Opus 5.5", efforts,
       {"claude-sonnet-5.5": "high", "claude-opus-5.5": "xhigh"})
    s, _, b = call(p, "POST", "/v1/messages", msg("claude-sonnet-4-6", stream=True))
    counts = [json.loads(call(p, "POST", "/v1/messages/count_tokens", {"model": n, "messages": USER})[2])
              for n in ("sonnet", "opus", "haiku")]
    ck("D5 stream forwarded unchanged; exact token count for all three",
       (s, b, counts, STATE["seen"][-1][0]), (200, "".join(NATIVE_SSE), [{"input_tokens": 42}] * 3,
                                               "/v1/messages/count_tokens"))
    refused = call(p, "POST", "/v1/chat/completions", {"model": "gpt-4.1", "messages": USER})[0]
    passed = call(p, "POST", "/v1/chat/completions", {"model": "claude-haiku-5.5", "messages": USER})[0]
    ck("D6 OpenAI route: another model is refused, a 5.5 model passes",
       (refused, passed, STATE["seen"][-1][0], STATE["seen"][-1][1]["model"]),
       (400, 200, "/chat/completions", "claude-haiku-5.5"))
    env = cp.claude_code_env("http://127.0.0.1:4141", p.catalog, None, None)
    ck("D7 printed Claude Code settings name the three 5.5 models", (
        env["ANTHROPIC_MODEL"], env["ANTHROPIC_DEFAULT_OPUS_MODEL"], env["ANTHROPIC_DEFAULT_HAIKU_MODEL"],
        env["CLAUDE_CODE_SUBAGENT_MODEL"], env["CLAUDE_CODE_AUTO_COMPACT_WINDOW"]),
       ("claude-sonnet-5.5", "claude-opus-5.5", "claude-haiku-5.5", "claude-haiku-5.5", "128000"))
    # an account without the 5.5 models (the usual fake list has no claude-haiku-5.5): Copilot refuses the model
    without = start_proxy(base, any_model=False)
    STATE["script"] = ["unsupported"]
    s, _, b = call(without, "POST", "/v1/messages", msg("claude-haiku-4-5"))
    text = json.loads(b)["error"]["message"] if s == 400 else b
    ck("D8 account without 5.5: Copilot's refusal comes back readable, with the way out", (
        s, text.startswith("The requested model is not supported. (model_not_supported)"), "--any-model" in text,
        "it lists claude-sonnet-5.5" in text, "\\" in text), (400, True, True, True, False))


@group
def security_controls(ck: Check, base: str) -> None:
    """The controls that held in the security scan of 2026-11-26; they must keep holding."""
    import io
    p = start_proxy(base)
    port = p.server_address[1]
    body = json.dumps(msg()).encode()
    post = b"POST /v1/messages HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: %d\r\n" % len(body)

    def get(host: Optional[str], version: bytes = b"HTTP/1.1") -> int:
        lines = [b"GET /v1/models " + version] + ([] if host is None else [b"Host: " + host.encode()])
        return raw_call(p, b"\r\n".join(lines) + b"\r\n\r\n")

    foreign = ["evil.example", "127.0.0.1.evil.example", "localhost.evil.example", "evil.example:%d" % port,
               "127.0.0.1:%d@evil.example" % port, "127.1", "0.0.0.0", "[::ffff:127.0.0.1]", "",
               "127.0.0.1 evil.example", "evil.example#127.0.0.1"]
    ck("SC1 Host: 11 foreign or malformed values, a missing header and a foreign first of two are refused",
       (sorted({get(h) for h in foreign}), get(None, b"HTTP/1.0"),
        raw_call(p, b"GET /v1/models HTTP/1.1\r\nHost: evil.example\r\nHost: 127.0.0.1\r\n\r\n")),
       ([403], 403, 403))
    ck("SC2 Host: the proxy's own names still work",
       [get(h) for h in ("127.0.0.1:%d" % port, "LOCALHOST", "localhost.", "[::1]:%d" % port)], [200] * 4)
    mark = len(STATE["seen"])
    origins = [raw_call(p, post + b"Host: 127.0.0.1\r\nOrigin: " + o.encode() + b"\r\n\r\n" + body)
               for o in ("https://evil.example", "null", "http://127.0.0.1:%d" % port)]
    ck("SC3 Origin: a browser POST from a foreign, null or even the proxy's own origin is refused, unsent",
       (origins, len(seen_since(mark))), ([403] * 3, 0))
    p.auth_token = "k3y-0123456789"
    try:
        def keyed(*headers: bytes, path: bytes = b"/v1/messages") -> int:
            return raw_call(p, post.replace(b"/v1/messages", path, 1) + b"Host: 127.0.0.1\r\n"
                            + b"".join(h + b"\r\n" for h in headers) + b"\r\n" + body)
        ck("SC4 client key: missing, wrong, empty, Basic or in the URL is refused; the right one works in both headers",
           [keyed(), keyed(b"x-api-key: wrong"), keyed(b"Authorization: Bearer "),
            keyed(b"Authorization: Basic k3y-0123456789"), keyed(path=b"/v1/messages?key=k3y-0123456789"),
            keyed(b"x-api-key: k3y-0123456789"), keyed(b"Authorization: bearer k3y-0123456789")],
           [401, 401, 401, 401, 401, 200, 200])
    finally:
        p.auth_token = None
    start = b"GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\n"
    ck("SC5 limits: 150 headers, a 70 KB header and a 70 KB request line are refused",
       (raw_call(p, start + b"".join(b"X-%d: 1\r\n" % i for i in range(150)) + b"\r\n"),
        raw_call(p, start + b"X-Big: " + b"a" * 70000 + b"\r\n\r\n"),
        raw_call(p, b"GET /" + b"a" * 70000 + b" HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")), (431, 431, 414))
    mark = len(STATE["seen"])
    answers = raw_exchange(p, post + b"Host: evil.example\r\n\r\n" + body + post + b"Host: 127.0.0.1\r\n\r\n" + body)
    ck("SC6 a request pipelined behind a refused one is not processed",
       (answers.count(b"HTTP/1.1 "), len(seen_since(mark))), (1, 0))
    raw_call(p, post + b"Host: 127.0.0.1\r\nanthropic-beta: a-1\r\n\tX-Evil: 1\r\n"
             b"Authorization: Bearer client-key\r\nCookie: sid=1\r\nx-api-key: client-key\r\n\r\n" + body)
    sent = {k.lower(): v for k, v in STATE["seen"][-1][2].items()}
    ck("SC7 a folded header adds no header upstream; the client's own credentials are not forwarded",
       ("x-evil" in sent, "cookie" in sent, "x-api-key" in sent, sent.get("authorization")),
       (False, False, False, "Bearer copilot-test"))
    hostile = Namespace(token_file='/tmp/a") (allow default) ("/tok', copilot_config_dir="/tmp/cfg",
                        no_copilot_config=False, usage_log=None, unix_socket=None)
    profile = cp.self_sandbox_profile(hostile)
    ck("SC8 --sandbox-self: a path with quotes cannot add a rule to the sandbox profile",
       ('a") (allow default)' in profile.replace('\\"', ""), 'a\\") (allow default) (\\"' in profile), (False, True))
    buf = io.StringIO()
    handler = cp.logging.StreamHandler(buf)
    cp.LOG.addHandler(handler)
    cp.logging.disable(cp.logging.NOTSET)
    level, cp.LOG.level = cp.LOG.level, cp.logging.DEBUG
    try:
        STATE["script"] = ["401", "", "429"]  # one entry per upstream request: 401, its retry, then a 429
        codes = [call(p, "POST", "/v1/messages", msg())[0], call(p, "POST", "/v1/messages", msg(CHAT))[0],
                 call(p, "GET", "/v1/models")[0],
                 call(p, "POST", "/v1/messages/count_tokens", {"model": NATIVE, "messages": USER})[0]]
        text = buf.getvalue()
    finally:
        cp.logging.disable(cp.logging.CRITICAL)
        cp.LOG.removeHandler(handler)
        cp.LOG.level = level
    ck("SC9 no token in the log at debug level (normal answers, a 401 with token refresh, a 429)",
       (codes, text.count("\n") > 8, "copilot-test" in text, "gh-test" in text),
       ([200, 429, 200, 200], True, False, False))
    openssl = shutil.which("openssl")  # only needed to make a throw-away certificate
    if openssl:
        with tempfile.TemporaryDirectory() as tmp:
            cert, key = os.path.join(tmp, "c.pem"), os.path.join(tmp, "k.pem")
            subprocess.run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert,
                            "-days", "1", "-subj", "/CN=127.0.0.1"], check=True, capture_output=True)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            tls = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
            tls.socket = context.wrap_socket(tls.socket, server_side=True)
            serve(tls)
            mark = len(STATE["seen"])
            mitm = start_proxy("https://127.0.0.1:%d" % tls.server_address[1], load=False)
            status, _, answer = call(mitm, "POST", "/v1/messages", msg(CHAT))
            ck("SC10 TLS: an upstream with a self-signed certificate is refused and gets nothing",
               (status, "CERTIFICATE_VERIFY_FAILED" in answer, len(seen_since(mark))), (502, True, 0))
            tls.shutdown()


@group
def hardening(ck: Check, base: str) -> None:
    """The findings of the security scan of 2026-11-26, one check each."""
    import io
    p = start_proxy(base)
    port = p.server_address[1]
    head = b"POST /v1/messages HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n"
    body = json.dumps(msg()).encode()

    # control characters from a client never reach the terminal
    buf = io.StringIO()
    handler = cp.logging.StreamHandler(buf)
    cp.LOG.addHandler(handler)
    cp.logging.disable(cp.logging.NOTSET)
    level, cp.LOG.level = cp.LOG.level, cp.logging.DEBUG
    try:
        raw_call(p, b"GET /\x1b[2Jcleared HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
        call(p, "POST", "/v1/messages", msg("x\u001b[2J\n12:00:00 [INFO] forged line"))
        text = buf.getvalue()
    finally:
        cp.logging.disable(cp.logging.CRITICAL)
        cp.LOG.removeHandler(handler)
        cp.LOG.level = level
    ck("H1 log: escape sequences and line breaks sent by a client are written as text",
       ("\x1b" in text, "\n12:00:00 [INFO] forged line" in text, "\\x1b[2J" in text, text.count("\n") > 2),
       (False, False, True, True))
    prompts: list[str] = []
    saved_input, builtins.input = builtins.input, lambda prompt="": (prompts.append(prompt), "n")[1]
    try:
        manual = start_proxy(DEAD, load=False)  # no model list: the client's model name is used as it came
        manual.guard.manual_approval = True
        status = call(manual, "POST", "/v1/messages", msg("claude-haiku-4.5\rFAKE: forward a harmless request"))[0]
    finally:
        builtins.input = saved_input
    ck("H2 --manual: the approval prompt shows a client's control characters as text",
       (status, len(prompts), "\r" in prompts[0], "\\x0dFAKE" in prompts[0]), (403, 1, False, True))

    # a client that stops sending is dropped; a normal request still works
    saved_timeout, cp.Handler.timeout = cp.Handler.timeout, 1
    try:
        started = time.time()
        with socket.create_connection(("127.0.0.1", port), timeout=6) as s1, \
                socket.create_connection(("127.0.0.1", port), timeout=6) as s2:
            s1.sendall(b"GET /health HTT")
            s2.sendall(head + b"Content-Length: 1000\r\n\r\n{")
            closed = (s1.recv(100), s2.recv(4096).split(b"\r\n")[0])
        waited = time.time() - started
        ck("H3 a connection that never finishes its request, or its body, is closed after the client timeout",
           (closed, waited < 5, call(p, "POST", "/v1/messages", msg())[0]),
           ((b"", b"HTTP/1.1 408 Request Timeout"), True, 200))
    finally:
        cp.Handler.timeout = saved_timeout

    saved_items, cp.MAX_JSON_ITEMS = getattr(cp, "MAX_JSON_ITEMS", None), 1000
    try:
        flood = msg(messages=[{"role": "user", "content": [[] for _ in range(2000)]}])
        ck("H4 a body with more JSON items than the limit is refused before it is parsed; a normal one passes",
           (call(p, "POST", "/v1/messages", flood)[0], call(p, "POST", "/v1/messages", msg())[0]), (413, 200))
    finally:
        cp.MAX_JSON_ITEMS = saved_items

    def site(value: Optional[str], proxy: Any = p, origin: Optional[str] = None) -> int:
        headers = {**({"Sec-Fetch-Site": value} if value else {}), **({"Origin": origin} if origin else {})}
        return call(proxy, "GET", "/stats", headers=headers)[0]

    cors = start_proxy(base)
    cors.cors_origins = {"https://app.example"}
    ck("H5 a browser GET from another site is refused even without an Origin header",
       (site("cross-site"), site("same-site"), site("none"), site("same-origin"), site(None),
        site("cross-site", cors, "https://app.example")), (403, 403, 200, 200, 200, 200))

    shown = serve(cp.ProxyServer(("127.0.0.1", 0), p.tm, p.catalog, cp.RequestGuard(None, False, False), True))
    no_key = call(shown, "GET", "/token")[0]
    shown.auth_token = "k"
    with_key = call(shown, "GET", "/token", headers={"x-api-key": "k"})
    health = json.loads(call(shown, "GET", "/health")[2])
    ck("H6 /token needs a client key even with --show-token; /health says nothing about the account when a key is set",
       (no_key, with_key[0], "copilot-test" in with_key[2], sorted(health),
        "api_base_url" in json.loads(call(p, "GET", "/health")[2])),
       (403, 200, True, ["service", "status"], True))

    chunked = b"%x\r\n%s\r\n0\r\n\r\n" % (len(body), body)
    ck("H7 request framing: Content-Length with Transfer-Encoding, and bare line feeds in a chunked body, get 400",
       (raw_call(p, head + b"Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n" + chunked),
        raw_call(p, head + b"Transfer-Encoding: chunked\r\n\r\n" + b"%x\n%s\n0\n\n" % (len(body), body)),
        raw_call(p, head + b"Transfer-Encoding: chunked\r\n\r\n" + chunked)), (400, 400, 200))

    real_request = cp.http_json_request
    answers = iter(["http://attacker.example", "https://githubcopilot.com.evil.example",
                    "https://api.business.githubcopilot.com/"])
    cp.http_json_request = lambda *a, **k: (200, {"token": "t", "expires_at": time.time() + 1000,
                                                  "endpoints": {"api": next(answers)}})
    try:
        tm = cp.TokenManager("gh-test")
        bases = [tm.get_token(force_refresh=True)[1] for _ in range(3)]
    finally:
        cp.http_json_request = real_request
    ck("H8 the API address from GitHub is used only when it is https on a GitHub Copilot host", bases,
       ["https://api.githubcopilot.com", "https://api.githubcopilot.com", "https://api.business.githubcopilot.com"])

    with tempfile.TemporaryDirectory() as tmp:
        old_mask = os.umask(0o022)
        try:
            cp.UsageLog(os.path.join(tmp, "usage.jsonl")).record(route="native", model="m", status=200)
        finally:
            os.umask(old_mask)
        ck("H9 a new usage log is readable by its owner only",
           stat.S_IMODE(os.stat(os.path.join(tmp, "usage.jsonl")).st_mode), 0o600)
    import struct
    saved_err, sys.stderr = sys.stderr, io.StringIO()
    try:
        for _ in range(5):  # connect and reset at once: the proxy is still waiting for the request line
            s = socket.create_connection(("127.0.0.1", port))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            s.close()
        time.sleep(0.5)
        noise = sys.stderr.getvalue()
    finally:
        sys.stderr = saved_err
    ck("H11 a client that resets its connection leaves no traceback on the terminal",
       ("Traceback" in noise, call(p, "GET", "/health")[0]), (False, 200))
    ck("H10 only loopback addresses may be served without a client key",
       [getattr(cp, "is_loopback", lambda h: None)(h) for h in (
           "127.0.0.1", "localhost", "::1", "127.9.9.9", "0.0.0.0", "::", "192.168.1.5", "example.com")],
       [True, True, True, True, False, False, False, False])


@group
def limits(ck: Check, base: str) -> None:
    r = start_proxy(base, rate=30.0)
    first = call(r, "POST", "/v1/messages", msg())[0]
    s, h, _ = call(r, "POST", "/v1/messages", msg())
    ck("R1 rate limit 429 + Retry-After", (first, s, h.get("retry-after", "").isdigit()), (200, 429, True))
    ck("R2 rate limit covers /v1/embeddings", call(r, "POST", "/v1/embeddings", {"input": "x"})[0], 429)
    m = start_proxy(base)
    m.guard.manual_approval = True
    saved, answers = builtins.input, ["n", "y"]
    builtins.input = lambda prompt="": answers.pop(0)
    try:
        ck("R3 --manual: refused = 403, approved = 200",
           (call(m, "POST", "/v1/messages", msg())[0], call(m, "POST", "/v1/messages", msg())[0]), (403, 200))
    finally:
        builtins.input = saved


@group
def pool(ck: Check, base: str) -> None:
    p = start_proxy(base)
    reset_pool()
    codes = [call(p, "POST", "/v1/messages", msg())[0],
             call(p, "POST", "/v1/messages", msg(stream=True))[0],
             call(p, "POST", "/v1/messages", msg(CHAT, stream=True))[0],
             call(p, "POST", "/v1/messages/count_tokens", {"model": NATIVE, "messages": USER})[0],
             call(p, "POST", "/v1/chat/completions", {"model": CHAT, "messages": USER, "stream": True})[0]]
    ck("K1 five requests in a row use one upstream connection", (codes, STATE["conns"]), ([200] * 5, 1))
    reset_pool()
    STATE["script"] = ["silent-close"]
    a = call(p, "POST", "/v1/messages", msg())[0]
    time.sleep(0.1)  # the fake upstream has closed the kept connection by now
    ck("K2 Copilot closed the kept connection: the next request still works",
       (a, call(p, "POST", "/v1/messages", msg())[0]), (200, 200))
    reset_pool()
    saved = getattr(cp, "_looks_open", None)
    cp._looks_open = lambda conn: True  # the close goes unnoticed until the request is sent
    try:
        STATE["script"], mark = ["silent-close"], len(STATE["seen"])
        a = call(p, "POST", "/v1/messages", msg())[0]
        time.sleep(0.1)
        ck("K3 closed connection noticed only while sending: one retry on a new connection",
           (a, call(p, "POST", "/v1/messages", msg())[0], len(seen_since(mark))), (200, 200, 2))
    finally:
        cp._looks_open = saved
    reset_pool()
    STATE["script"] = ["close-header"]
    a = call(p, "POST", "/v1/messages/count_tokens", {"model": NATIVE, "messages": USER})[0]
    ck("K4 'Connection: close' from Copilot: that connection is not reused",
       (a, call(p, "POST", "/v1/messages", msg())[0], STATE["conns"]), (200, 200, 2))
    reset_pool()
    codes = []

    def worker() -> None:
        for _ in range(4):
            codes.append(call(p, "POST", "/v1/messages", msg(stream=True))[0])
    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ck("K5 24 requests from 6 threads: all answered, at most 6 upstream connections",
       (codes.count(200), STATE["conns"] <= 6), (24, True))


@group
def listeners(ck: Check, base: str) -> None:
    if socket.has_ipv6:
        try:
            six = start_proxy(base, addr=("::1", 0))
            ck("S1 listens on IPv6 (-H ::1)", call(six, "GET", "/health")[0], 200)
        except OSError as exc:
            ck("S1 listens on IPv6 (-H ::1)", f"{type(exc).__name__}: {exc}", 200)
    if not hasattr(socket, "AF_UNIX"):
        return
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "run", "p.sock")
        tm = FakeTokens(base)
        catalog = cp.ModelCatalog(tm, any_model=True)
        catalog.get_models_payload(force=True)
        srv = serve(cp.UnixProxyServer(path, tm, catalog, cp.RequestGuard(None, False, False), False))

        class UnixConnection(http.client.HTTPConnection):
            def connect(self) -> None:
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.connect(path)

        conn = UnixConnection("localhost", timeout=10)
        conn.request("POST", "/v1/messages", body=json.dumps(msg(stream=True)).encode(),
                     headers={"Content-Type": "application/json"})
        r = conn.getresponse()
        body = r.read().decode()
        conn.request("GET", "/health")  # same connection again
        again = conn.getresponse()
        again.read()
        conn.close()
        mode = stat.S_IMODE(os.stat(path).st_mode)
        srv.shutdown()
        srv.server_close()
        ck("S2 Unix socket: stream, second request on the same connection, mode 0600, file removed at stop",
           (r.status, body, again.status, mode, os.path.exists(path)), (200, "".join(NATIVE_SSE), 200, 0o600, False))


def main() -> int:
    cp.logging.disable(cp.logging.CRITICAL)
    wanted = sys.argv[1:]
    up = serve(ThreadingHTTPServer(("127.0.0.1", 0), Upstream))
    base = f"http://127.0.0.1:{up.server_address[1]}"
    results: list[tuple[str, bool, str]] = []

    def ck(name: str, got: Any, want: Any) -> None:
        results.append((name, got == want, f"got {got!r}, want {want!r}"))

    for fn in GROUPS:
        if wanted and not any(w in fn.__name__ for w in wanted):
            continue
        STATE["script"] = []
        try:
            fn(ck, base)
        except Exception as exc:  # a crash in one group is a failed check, not a lost run
            results.append((f"{fn.__name__}: stopped", False, f"{type(exc).__name__}: {exc}"))
    failed = [r for r in results if not r[1]]
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'} {name}" + ("" if ok else f" ({detail})"))
    print(f"{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
