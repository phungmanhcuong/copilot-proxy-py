#!/usr/bin/env python3
"""
copilot_proxy.py - single-file, standard-library-only proxy that lets the Claude Code CLI
(Anthropic Messages API) and OpenAI clients use a GitHub Copilot subscription.

Python port of the design of github.com/ericc-ch/copilot-api, with these additions:
  * an upstream error after streaming started is sent as an Anthropic `event: error`,
    not as a second HTTP response inside the open stream;
  * tool calls are buffered per index and emitted as complete blocks, so argument
    deltas can never land on a closed block;
  * `--manual` approval prompts are serialised with one lock;
  * default models come from the live /models catalog, printed as the current
    Claude Code variables (ANTHROPIC_DEFAULT_*_MODEL);
  * an unknown model re-fetches the catalog and falls back to a real chat model;
  * an existing Copilot editor login (~/.config/github-copilot/apps.json) is reused.

Requires Python 3.9+. No third-party packages.
"""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GITHUB_CLIENT_ID = "Iv1.b507a08c87ecfe98"  # VS Code Copilot client id (same as copilot-api)
GITHUB_BASE_URL = "https://github.com"
GITHUB_API_BASE_URL = "https://api.github.com"

COPILOT_API_URLS = {
    "individual": "https://api.githubcopilot.com",
    "business": "https://api.business.githubcopilot.com",
    "enterprise": "https://api.enterprise.githubcopilot.com",
}

EDITOR_VERSION = "vscode/1.99.0"
EDITOR_PLUGIN_VERSION = "copilot-chat/0.26.7"
COPILOT_USER_AGENT = "GitHubCopilotChat/0.26.7"
GITHUB_API_VERSION = "2025-04-01"

# Default Claude models (Claude 5.5 family), used when the account's /models lists them; otherwise the newest
# listed version of the family. Override with COPILOT_PROXY_DEFAULT_{SONNET,OPUS,HAIKU} or -m / -s.
PREFERRED_MODELS = {
    "sonnet": os.environ.get("COPILOT_PROXY_DEFAULT_SONNET", "claude-sonnet-5.5"),
    "opus": os.environ.get("COPILOT_PROXY_DEFAULT_OPUS", "claude-opus-5.5"),
    "haiku": os.environ.get("COPILOT_PROXY_DEFAULT_HAIKU", "claude-haiku-5.5"),
}

DEFAULT_PORT = 4141
DEFAULT_HOST = "127.0.0.1"
# Upstream 429/5xx retries before streaming. Default 0: Claude Code's SDK already retries on Retry-After (which
# the proxy forwards), and a rejected 429/5xx costs no tokens, so proxy-side retries only help other clients.
try:
    RETRIES = max(0, min(5, int(os.environ.get("COPILOT_PROXY_RETRIES", "0"))))
except ValueError:
    RETRIES = 0
RETRY_STATUSES = (429, 500, 502, 503, 504, 529)
MAX_RETRY_WAIT = 20.0  # longer Retry-After values go back to the client instead of blocking here
CONNECT_TIMEOUT = 15
REQUEST_TIMEOUT = 600

LOG = logging.getLogger("copilot-proxy")


def default_token_path() -> Path:
    xdg = os.environ.get("XDG_DATA_HOME")
    base = Path(xdg) if xdg else Path.home() / ".local" / "share"
    return base / "copilot-api" / "github_token"


def default_copilot_config_dir() -> Path:
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "github-copilot"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_SSL = ssl.create_default_context()


def http_json_request(
    method: str,
    url: str,
    headers: Optional[dict[str, str]] = None,
    payload: Optional[dict[str, Any]] = None,
    timeout: int = 30,
) -> tuple[int, dict[str, str], Any]:
    """JSON request via urllib. Returns (status, headers, parsed body); never raises on HTTP errors."""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req_headers = {"Accept": "application/json"}
    if data is not None:
        req_headers["Content-Type"] = "application/json"
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)

    def parse(raw: str, fallback: Any) -> Any:
        try:
            return json.loads(raw) if raw else fallback
        except json.JSONDecodeError:
            return {"raw": raw}

    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, dict(resp.headers.items()), parse(raw, {})
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        hdrs = dict(exc.headers.items()) if exc.headers else {}
        return exc.code, hdrs, parse(raw, {"error": str(exc.reason)})


def _retry_after(header: Optional[str], attempt: int) -> Optional[float]:
    """Seconds to wait before a retry, or None when the server asks for longer than MAX_RETRY_WAIT."""
    if header:
        try:
            wait = float(header)
        except ValueError:
            return None  # HTTP-date form: let the client handle it
        if wait != wait or wait < 0 or wait > MAX_RETRY_WAIT:  # NaN, negative, or too long
            return None
        return wait
    return min(MAX_RETRY_WAIT, 1.5 * (2 ** attempt))


def _json_or_none(raw: Any) -> dict[str, Any]:
    """Parse JSON bytes/str; anything that is not a JSON object becomes {}."""
    try:
        v = json.loads(raw) if raw else {}
    except (ValueError, TypeError):
        return {}
    return v if isinstance(v, dict) else {}


def _dict_get(d: Any, key: str) -> dict[str, Any]:
    v = d.get(key) if isinstance(d, dict) else None
    return v if isinstance(v, dict) else {}


def _route_unsupported(status: int, raw: bytes) -> bool:
    """Copilot refused the native endpoint or model itself (not the request content)."""
    if status in (404, 405, 415):
        return True
    if status in (400, 422):
        text = raw[:2000].decode("utf-8", errors="replace").lower()
        return any(s in text for s in ("not supported", "unsupported", "unknown endpoint", "model_not_supported"))
    return False


_HOST_RE = re.compile(r"^(?:\[([0-9a-f:.]+)\]|([a-z0-9.-]+))(?::(\d{1,5}))?$")


def host_name(value: str) -> Optional[str]:
    """Normalised host part of a Host header or --allow-host value ("[::1]", "localhost"); None if malformed."""
    v = (value or "").strip().lower()
    if v.count(":") > 1 and not v.startswith("["):
        v = f"[{v}]"  # bare IPv6 from --host/--allow-host
    m = _HOST_RE.match(v)
    if not m:
        return None
    return f"[{m.group(1)}]" if m.group(1) else m.group(2).rstrip(".")


def open_upstream(
    base_url: str, path: str, method: str, headers: dict[str, str], body: Optional[bytes]
) -> tuple[http.client.HTTPConnection, http.client.HTTPResponse]:
    """Raw connection so a streamed (SSE) body can be read line by line."""
    parsed = urllib.parse.urlsplit(base_url)
    host = parsed.hostname or ""
    full_path = parsed.path.rstrip("/") + path
    if parsed.scheme == "https":
        conn: http.client.HTTPConnection = http.client.HTTPSConnection(
            host, parsed.port or 443, timeout=REQUEST_TIMEOUT, context=_SSL
        )
    else:
        conn = http.client.HTTPConnection(host, parsed.port or 80, timeout=REQUEST_TIMEOUT)
    conn.request(method, full_path, body=body, headers=headers)
    return conn, conn.getresponse()


# ---------------------------------------------------------------------------
# GitHub login and Copilot token
# ---------------------------------------------------------------------------


def github_headers(github_token: Optional[str] = None) -> dict[str, str]:
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Editor-Version": EDITOR_VERSION,
        "Editor-Plugin-Version": EDITOR_PLUGIN_VERSION,
        "User-Agent": COPILOT_USER_AGENT,
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    if github_token:
        h["Authorization"] = f"token {github_token}"
    return h


def load_github_token(path: Path) -> Optional[str]:
    if path.exists():
        tok = path.read_text(encoding="utf-8").strip()
        return tok or None
    return None


def save_github_token(path: Path, token: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(token.strip() + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def copilot_token_works(github_token: str) -> tuple[bool, int]:
    status, _, data = http_json_request(
        "GET",
        f"{GITHUB_API_BASE_URL}/copilot_internal/v2/token",
        headers=github_headers(github_token),
        timeout=CONNECT_TIMEOUT,
    )
    return status == 200 and isinstance(data, dict) and "token" in data, status


def token_from_copilot_config(config_dir: Path) -> Optional[str]:
    """Reuse a login made by a Copilot editor plugin (apps.json / hosts.json). First working token wins."""
    candidates: list[tuple[str, str]] = []

    def walk(obj: Any, where: str) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                walk(v, f"{where}/{k}")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                walk(v, f"{where}[{i}]")
        elif isinstance(obj, str) and re.match(r"^(ghu|gho|ghp|github_pat)_", obj):
            candidates.append((where, obj))

    for name in ("apps.json", "hosts.json"):
        p = config_dir / name
        if not p.exists():
            continue
        try:
            walk(json.loads(p.read_text(encoding="utf-8")), name)
        except (OSError, json.JSONDecodeError) as exc:
            LOG.warning("Cannot read %s: %s", p, exc)

    for where, tok in candidates:
        ok, status = copilot_token_works(tok)
        LOG.info("Copilot config token %s (%s...): %s", where.split("/oauth_token")[0], tok[:4], status)
        if ok:
            return tok
    return None


def run_device_auth_flow(token_path: Path, show_token: bool = False) -> str:
    """GitHub OAuth device flow (RFC 8628)."""
    status, _, data = http_json_request(
        "POST",
        f"{GITHUB_BASE_URL}/login/device/code",
        headers=github_headers(),
        payload={"client_id": GITHUB_CLIENT_ID, "scope": "read:user"},
    )
    if status != 200 or "device_code" not in data:
        raise RuntimeError(f"GitHub device flow start failed ({status}): {data}")

    interval = int(data.get("interval", 5))
    expires_at = time.time() + int(data.get("expires_in", 900))
    print("\nGitHub Copilot login")
    print(f"1. Open: {data['verification_uri']}")
    print(f"2. Code: {data['user_code']}")
    print("Waiting for authorization...\n", flush=True)

    while time.time() < expires_at:
        time.sleep(interval)
        _, _, p = http_json_request(
            "POST",
            f"{GITHUB_BASE_URL}/login/oauth/access_token",
            payload={
                "client_id": GITHUB_CLIENT_ID,
                "device_code": data["device_code"],
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            },
        )
        if isinstance(p, dict) and p.get("access_token"):
            save_github_token(token_path, p["access_token"])
            print(f"Login succeeded; token saved to {token_path}")
            if show_token:
                print(f"GitHub token: {p['access_token']}")
            return p["access_token"]
        err = p.get("error") if isinstance(p, dict) else None
        if err == "authorization_pending":
            continue
        if err == "slow_down":
            interval += 5
            continue
        raise RuntimeError(f"GitHub device flow failed: {p}")
    raise TimeoutError("GitHub device authorization timed out.")


class TokenManager:
    """Caches the short-lived Copilot token and refreshes it in the background."""

    def __init__(self, github_token: str, account_type: str = "individual", show_token: bool = False):
        self.github_token = github_token
        self.show_token = show_token
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at = 0.0
        self._base_url = COPILOT_API_URLS.get(account_type, COPILOT_API_URLS["individual"])
        self._stop = threading.Event()

    @property
    def api_base_url(self) -> str:
        with self._lock:
            return self._base_url

    def get_token(self, force_refresh: bool = False) -> tuple[str, str]:
        with self._lock:
            if not force_refresh and self._token and time.time() < self._expires_at - 60:
                return self._token, self._base_url
            return self._refresh_locked()

    def _refresh_locked(self) -> tuple[str, str]:
        status, _, data = http_json_request(
            "GET",
            f"{GITHUB_API_BASE_URL}/copilot_internal/v2/token",
            headers=github_headers(self.github_token),
            timeout=CONNECT_TIMEOUT,
        )
        if status != 200 or "token" not in data:
            raise RuntimeError(f"GitHub -> Copilot token exchange failed ({status}): {data}")
        self._token = data["token"]
        exp = data.get("expires_at")
        if isinstance(exp, (int, float)) and exp > time.time():
            self._expires_at = float(exp)
        else:
            self._expires_at = time.time() + int(data.get("refresh_in", 1500))
        endpoints = data.get("endpoints")
        if isinstance(endpoints, dict) and endpoints.get("api"):
            self._base_url = str(endpoints["api"]).rstrip("/")
        LOG.info("Copilot token refreshed (base=%s, expires_in=%ds)",
                 self._base_url, int(self._expires_at - time.time()))
        if self.show_token:
            LOG.info("Copilot token: %s", self._token)
        return self._token, self._base_url

    def start_background_refresh(self) -> None:
        self.get_token(force_refresh=True)

        def loop() -> None:
            while not self._stop.is_set():
                with self._lock:
                    wait = max(30.0, self._expires_at - time.time() - 90.0)
                if self._stop.wait(timeout=wait):
                    break
                try:
                    self.get_token(force_refresh=True)
                except Exception as exc:  # keep the loop alive
                    LOG.warning("Background token refresh failed: %s", exc)
                    self._stop.wait(timeout=30.0)

        threading.Thread(target=loop, name="copilot-token-refresh", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def fetch_usage(self) -> tuple[int, Any]:
        status, _, data = http_json_request(
            "GET",
            f"{GITHUB_API_BASE_URL}/copilot_internal/user",
            headers=github_headers(self.github_token),
            timeout=CONNECT_TIMEOUT,
        )
        return status, data

    @staticmethod
    def copilot_headers(token: str, is_agent: bool = False, is_vision: bool = False) -> dict[str, str]:
        h = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Copilot-Integration-Id": "vscode-chat",
            "Editor-Version": EDITOR_VERSION,
            "Editor-Plugin-Version": EDITOR_PLUGIN_VERSION,
            "User-Agent": COPILOT_USER_AGENT,
            "Openai-Intent": "conversation-panel",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
            "X-Initiator": "agent" if is_agent else "user",
            "X-Request-Id": str(uuid.uuid4()),
        }
        if is_vision:
            h["Copilot-Vision-Request"] = "true"
        return h


# ---------------------------------------------------------------------------
# Rate limit and manual approval
# ---------------------------------------------------------------------------

_APPROVAL_LOCK = threading.Lock()


class RequestGuard:
    def __init__(self, rate_limit_seconds: Optional[float], wait_on_limit: bool, manual_approval: bool):
        self.rate_limit_seconds = rate_limit_seconds
        self.wait_on_limit = wait_on_limit
        self.manual_approval = manual_approval
        self._lock = threading.Lock()
        self._last = 0.0

    def check(self, summary: str) -> tuple[bool, Optional[float]]:
        """(allowed, retry_after). retry_after None + not allowed = rejected by the operator."""
        if self.rate_limit_seconds and self.rate_limit_seconds > 0:
            with self._lock:
                elapsed = time.time() - self._last
                if self._last > 0 and elapsed < self.rate_limit_seconds:
                    remaining = self.rate_limit_seconds - elapsed
                    if not self.wait_on_limit:
                        return False, remaining
                    LOG.info("Rate limit: waiting %.1fs", remaining)
                    time.sleep(remaining)
                self._last = time.time()

        if self.manual_approval:
            with _APPROVAL_LOCK:  # one prompt at a time; Claude Code sends requests in parallel
                try:
                    ans = input(f"\n[APPROVAL] Forward {summary}? [y/N]: ")
                except EOFError:
                    ans = "n"
            if ans.strip().lower() not in ("y", "yes"):
                return False, None
        return True, None


# ---------------------------------------------------------------------------
# Model catalog and model-id mapping
# ---------------------------------------------------------------------------


def _version_key(model_id: str) -> tuple:
    return tuple(int(n) for n in re.findall(r"\d+", model_id))


class ModelCatalog:
    def __init__(self, tm: TokenManager, primary: Optional[str] = None, small: Optional[str] = None):
        self.tm = tm
        self.primary = primary
        self.small = small
        self._lock = threading.Lock()
        self._payload: dict[str, Any] = {"object": "list", "data": []}
        self._models: dict[str, dict[str, Any]] = {}
        self._fetched = 0.0

    def get_models_payload(self, force: bool = False) -> dict[str, Any]:
        with self._lock:
            if not force and self._models and time.time() - self._fetched < 300:
                return self._payload
        token, base = self.tm.get_token()
        status, _, data = http_json_request(
            "GET", f"{base}/models", headers=self.tm.copilot_headers(token), timeout=CONNECT_TIMEOUT
        )
        if status == 401:
            token, base = self.tm.get_token(force_refresh=True)
            status, _, data = http_json_request(
                "GET", f"{base}/models", headers=self.tm.copilot_headers(token), timeout=CONNECT_TIMEOUT
            )
        if status == 200 and isinstance(data, dict) and isinstance(data.get("data"), list):
            with self._lock:
                self._payload = data
                self._models = {m["id"]: m for m in data["data"] if isinstance(m, dict) and "id" in m}
                self._fetched = time.time()
            LOG.info("Loaded %d Copilot models", len(self._models))
        else:
            LOG.warning("Cannot fetch Copilot models (%s): %s", status, str(data)[:300])
        with self._lock:
            return self._payload

    def chat_model_ids(self) -> list[str]:
        with self._lock:
            models = dict(self._models)
        ids = [
            mid for mid, m in models.items()
            if (m.get("capabilities") or {}).get("type", "chat") == "chat"
        ]
        return sorted(ids)

    def newest(self, family: str) -> Optional[str]:
        """Default model for a family: the preferred Claude 5.5 id when this account's catalog serves it,
        otherwise the newest version Copilot lists (e.g. legacy annual plans, which get no new models)."""
        ids = [m for m in self.chat_model_ids() if m.startswith("claude") and family in m]
        preferred = PREFERRED_MODELS.get(family)
        if preferred and preferred in ids:
            return preferred
        return max(ids, key=_version_key) if ids else None

    def max_output_tokens(self, model_id: str) -> Optional[int]:
        with self._lock:
            m = self._models.get(model_id) or {}
        limit = ((m.get("capabilities") or {}).get("limits") or {}).get("max_output_tokens")
        return int(limit) if isinstance(limit, (int, float)) and limit > 0 else None

    def max_prompt_tokens(self, model_id: str) -> Optional[int]:
        with self._lock:
            m = self._models.get(model_id) or {}
        limit = ((m.get("capabilities") or {}).get("limits") or {}).get("max_prompt_tokens")
        return int(limit) if isinstance(limit, (int, float)) and limit > 0 else None

    def supports_endpoint(self, model_id: str, endpoint: str) -> bool:
        with self._lock:
            m = self._models.get(model_id) or {}
        return endpoint in (m.get("supported_endpoints") or [])

    def resolve_model(self, requested: str) -> str:
        """Map Anthropic ids (dated, hyphenated) to Copilot ids (dotted)."""
        low = (requested or "").lower()
        if self.small and "haiku" in low:
            return self.small
        if self.primary and "haiku" not in low:
            return self.primary

        available = set(self.chat_model_ids())
        if not available:
            self.get_models_payload(force=True)
            available = set(self.chat_model_ids())
        if requested in available:
            return requested

        stripped = re.sub(r"-\d{8}$", "", requested or "").replace("-latest", "")
        stripped = re.sub(r"\[.*\]$", "", stripped)  # e.g. "claude-x[1m]"
        dotted = re.sub(r"-(\d+)-(\d+)", r"-\1.\2", stripped)
        cands = [stripped, dotted]
        m = re.match(r"^claude-(sonnet|opus|haiku)-(\d+)(?:\.(\d+))?$", dotted)
        if m:
            fam, maj, minor = m.groups()
            if minor:
                cands.append(f"claude-{maj}.{minor}-{fam}")
            cands.append(f"claude-{fam}-{maj}")
        for c in cands:
            if c in available:
                LOG.debug("Model %s -> %s", requested, c)
                return c

        for fam in ("opus", "sonnet", "haiku"):
            if fam in low:
                best = self.newest(fam)
                if best:
                    LOG.info("Model %s not served; using newest %s: %s", requested, fam, best)
                    return best

        fallback = self.newest("sonnet") or (sorted(available)[0] if available else None)
        if fallback:
            LOG.warning("Unknown model %s; using %s", requested, fallback)
            return fallback
        return dotted or PREFERRED_MODELS["sonnet"]


# ---------------------------------------------------------------------------
# Anthropic -> OpenAI request
# ---------------------------------------------------------------------------


def _blocks_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    parts: list[str] = []
    if isinstance(content, list):
        for b in content:
            if isinstance(b, str):
                parts.append(b)
            elif isinstance(b, dict):
                if b.get("type") == "text" and isinstance(b.get("text"), str):
                    parts.append(b["text"])
                elif b.get("type") == "tool_result":
                    parts.append(_blocks_text(b.get("content", "")))
    return "\n".join(p for p in parts if p)


def _image_part(block: dict[str, Any]) -> Optional[dict[str, Any]]:
    src = block.get("source")
    if not isinstance(src, dict):
        return None
    if src.get("type") == "base64":
        return {"type": "image_url",
                "image_url": {"url": f"data:{src.get('media_type', 'image/png')};base64,{src.get('data', '')}"}}
    if src.get("type") == "url" and src.get("url"):
        return {"type": "image_url", "image_url": {"url": src["url"]}}
    return None


def anthropic_to_openai_request(
    payload: dict[str, Any], model: str, max_out: Optional[int] = None
) -> tuple[dict[str, Any], bool, bool]:
    """Returns (openai_payload, is_agent_initiator, is_vision)."""
    msgs: list[dict[str, Any]] = []
    is_agent = False
    is_vision = False

    system = payload.get("system")
    if isinstance(system, str) and system:
        msgs.append({"role": "system", "content": system})
    elif isinstance(system, list):
        text = "\n\n".join(b.get("text", "") for b in system
                           if isinstance(b, dict) and b.get("type") == "text" and b.get("text"))
        if text:
            msgs.append({"role": "system", "content": text})

    for msg in payload.get("messages", []):
        role = msg.get("role", "user")
        content = msg.get("content", "")

        if role == "assistant":
            if isinstance(content, str):
                msgs.append({"role": "assistant", "content": content})
                continue
            texts: list[str] = []
            calls: list[dict[str, Any]] = []
            for b in content if isinstance(content, list) else []:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "text":
                    texts.append(b.get("text", ""))
                elif b.get("type") == "tool_use":
                    inp = b.get("input", {})
                    calls.append({
                        "id": b.get("id") or f"call_{uuid.uuid4().hex[:16]}",
                        "type": "function",
                        "function": {"name": b.get("name", ""),
                                     "arguments": inp if isinstance(inp, str) else json.dumps(inp, ensure_ascii=False)},
                    })
                # thinking / redacted_thinking blocks are dropped: the OpenAI schema has no slot for them
            joined = "\n".join(t for t in texts if t)
            am: dict[str, Any] = {"role": "assistant", "content": joined or (None if calls else "")}
            if calls:
                am["tool_calls"] = calls
            msgs.append(am)
            continue

        if isinstance(content, str):
            msgs.append({"role": "user", "content": content})
            continue

        tool_msgs: list[dict[str, Any]] = []
        parts: list[dict[str, Any]] = []
        has_image = False
        for b in content if isinstance(content, list) else []:
            if not isinstance(b, dict):
                continue
            t = b.get("type")
            if t == "tool_result":
                text = _blocks_text(b.get("content", ""))
                if b.get("is_error") and not text.startswith("Error"):
                    text = f"Error: {text}" if text else "Error"
                tool_msgs.append({"role": "tool", "tool_call_id": b.get("tool_use_id", ""), "content": text})
                # images inside a tool result (e.g. screenshots) go into the following user message
                inner = b.get("content")
                if isinstance(inner, list):
                    for ib in inner:
                        if isinstance(ib, dict) and ib.get("type") == "image":
                            img = _image_part(ib)
                            if img:
                                parts.append(img)
                                has_image = True
            elif t == "text":
                parts.append({"type": "text", "text": b.get("text", "")})
            elif t == "image":
                img = _image_part(b)
                if img:
                    parts.append(img)
                    has_image = True
        is_vision = is_vision or has_image
        msgs.extend(tool_msgs)  # tool messages must directly follow the assistant tool_calls
        if parts:
            if has_image:
                msgs.append({"role": "user", "content": parts})
            else:
                msgs.append({"role": "user", "content": "\n".join(p["text"] for p in parts)})

    out: dict[str, Any] = {"model": model, "messages": msgs, "stream": bool(payload.get("stream"))}
    if out["stream"]:
        out["stream_options"] = {"include_usage": True}
    if payload.get("max_tokens") is not None:
        mt = int(payload["max_tokens"])
        out["max_tokens"] = min(mt, max_out) if max_out else mt
    for k in ("temperature", "top_p"):
        if payload.get(k) is not None:
            out[k] = payload[k]
    if payload.get("stop_sequences"):
        out["stop"] = payload["stop_sequences"]
    meta = payload.get("metadata")
    if isinstance(meta, dict) and meta.get("user_id"):
        out["user"] = str(meta["user_id"])[:64]

    tools = []
    for tool in payload.get("tools") or []:
        if not isinstance(tool, dict) or "name" not in tool:
            continue
        if "input_schema" not in tool and tool.get("type") not in (None, "custom"):
            continue  # Anthropic server tools (web_search, ...) have no OpenAI equivalent
        fn: dict[str, Any] = {"name": tool["name"],
                              "parameters": tool.get("input_schema") or {"type": "object", "properties": {}}}
        if tool.get("description"):
            fn["description"] = tool["description"]
        tools.append({"type": "function", "function": fn})
    if tools:
        out["tools"] = tools
        tc = payload.get("tool_choice")
        if isinstance(tc, dict):
            kind = tc.get("type")
            if kind in ("auto", "none"):
                out["tool_choice"] = kind
            elif kind == "any":
                out["tool_choice"] = "required"
            elif kind == "tool" and tc.get("name"):
                out["tool_choice"] = {"type": "function", "function": {"name": tc["name"]}}
    is_agent = anthropic_flags(payload)[0]  # one initiator rule for both routes
    return out, is_agent, is_vision


# ---------------------------------------------------------------------------
# OpenAI -> Anthropic response
# ---------------------------------------------------------------------------

FINISH_MAP = {"stop": "end_turn", "length": "max_tokens", "tool_calls": "tool_use",
              "function_call": "tool_use", "content_filter": "end_turn"}


def map_finish_reason(reason: Optional[str]) -> str:
    return FINISH_MAP.get(reason or "", "end_turn")


def anthropic_usage(usage: Any) -> dict[str, int]:
    if not isinstance(usage, dict):
        return {"input_tokens": 0, "output_tokens": 0}
    prompt = int(usage.get("prompt_tokens") or 0)
    cached = int(((usage.get("prompt_tokens_details") or {}).get("cached_tokens")) or 0)
    out = {"input_tokens": max(0, prompt - cached), "output_tokens": int(usage.get("completion_tokens") or 0)}
    if cached:
        out["cache_read_input_tokens"] = cached
    return out


def openai_usage(u: dict[str, Any]) -> dict[str, int]:
    """Chat Completions or Responses usage -> Anthropic-style fields (input excludes cached tokens)."""
    if not u:
        return {}
    if "prompt_tokens" in u:
        return anthropic_usage(u)
    try:
        cached = int(_dict_get(u, "input_tokens_details").get("cached_tokens") or 0)
        return {"input_tokens": max(0, int(u.get("input_tokens") or 0) - cached),
                "cache_read_input_tokens": cached, "output_tokens": int(u.get("output_tokens") or 0)}
    except (TypeError, ValueError):
        return {}


def _parse_args(raw: Any) -> Any:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}
    return {}


def _msg_id(raw: Any) -> str:
    rid = str(raw or uuid.uuid4().hex[:24])
    return rid if rid.startswith("msg_") else f"msg_{rid}"


def openai_to_anthropic_response(resp: dict[str, Any], model: str) -> dict[str, Any]:
    choices = resp.get("choices") or []
    blocks: list[dict[str, Any]] = []
    finish = None
    # Copilot can split one answer over several choices (text in one, tool calls in another)
    for ch in choices:
        m = ch.get("message") or {}
        finish = ch.get("finish_reason") or finish
        reasoning = m.get("reasoning_content") or m.get("reasoning_text")
        if isinstance(reasoning, str) and reasoning:
            blocks.append({"type": "thinking", "thinking": reasoning, "signature": ""})
        if isinstance(m.get("content"), str) and m["content"]:
            blocks.append({"type": "text", "text": m["content"]})
        for tc in m.get("tool_calls") or []:
            fn = tc.get("function") or {}
            blocks.append({"type": "tool_use", "id": tc.get("id") or f"toolu_{uuid.uuid4().hex[:16]}",
                           "name": fn.get("name", ""), "input": _parse_args(fn.get("arguments"))})
    if any(b["type"] == "tool_use" for b in blocks):
        finish = "tool_calls"
    if not blocks:
        blocks.append({"type": "text", "text": ""})
    return {"id": _msg_id(resp.get("id")), "type": "message", "role": "assistant", "model": model,
            "content": blocks, "stop_reason": map_finish_reason(finish), "stop_sequence": None,
            "usage": anthropic_usage(resp.get("usage"))}


def sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, separators=(',', ':'))}\n\n".encode()


class StreamTranslator:
    """OpenAI chat.completion.chunk stream -> Anthropic Messages stream.

    Thinking and text stream live. Tool calls are buffered per OpenAI index and emitted as
    complete tool_use blocks at the end, so a late argument delta can never hit a closed block.
    """

    def __init__(self, model: str):
        self.model = model
        self.msg_id = _msg_id(None)
        self.started = False
        self.finished = False
        self.next_index = 0
        self.open_index: Optional[int] = None
        self.open_type: Optional[str] = None
        self.tools: dict[int, dict[str, Any]] = {}
        self.stop_reason = "end_turn"
        self.usage: dict[str, int] = {"input_tokens": 0, "output_tokens": 0}

    def _start(self, ev: list[bytes]) -> None:
        if self.started:
            return
        self.started = True
        ev.append(sse("message_start", {"type": "message_start", "message": {
            "id": self.msg_id, "type": "message", "role": "assistant", "content": [], "model": self.model,
            "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 0, "output_tokens": 0}}}))

    def _close(self, ev: list[bytes]) -> None:
        if self.open_index is not None:
            if self.open_type == "thinking":
                ev.append(sse("content_block_delta", {"type": "content_block_delta", "index": self.open_index,
                                                      "delta": {"type": "signature_delta", "signature": ""}}))
            ev.append(sse("content_block_stop", {"type": "content_block_stop", "index": self.open_index}))
            self.open_index = None
            self.open_type = None

    def _delta(self, ev: list[bytes], kind: str, payload: dict[str, Any]) -> None:
        if self.open_type != kind:
            self._close(ev)
            self.open_index = self.next_index
            self.next_index += 1
            self.open_type = kind
            empty = {"type": "thinking", "thinking": ""} if kind == "thinking" else {"type": "text", "text": ""}
            ev.append(sse("content_block_start", {"type": "content_block_start", "index": self.open_index,
                                                  "content_block": empty}))
        ev.append(sse("content_block_delta", {"type": "content_block_delta", "index": self.open_index,
                                              "delta": payload}))

    def feed(self, chunk: dict[str, Any]) -> list[bytes]:
        ev: list[bytes] = []
        if chunk.get("id") and not self.started:
            self.msg_id = _msg_id(chunk["id"])
        if isinstance(chunk.get("usage"), dict):
            u = anthropic_usage(chunk["usage"])
            self.usage.update({k: v for k, v in u.items() if v})
        for choice in chunk.get("choices") or []:
            self._start(ev)
            d = choice.get("delta") or {}
            r = d.get("reasoning_content") or d.get("reasoning_text")
            if isinstance(r, str) and r:
                self._delta(ev, "thinking", {"type": "thinking_delta", "thinking": r})
            if isinstance(d.get("content"), str) and d["content"]:
                self._delta(ev, "text", {"type": "text_delta", "text": d["content"]})
            for tc in d.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                slot = self.tools.setdefault(int(tc.get("index", 0)), {"id": None, "name": "", "args": []})
                fn = tc.get("function") or {}
                if tc.get("id"):
                    slot["id"] = tc["id"]
                if fn.get("name"):
                    slot["name"] = slot["name"] or fn["name"]
                if isinstance(fn.get("arguments"), str):
                    slot["args"].append(fn["arguments"])
            if choice.get("finish_reason"):
                self.stop_reason = map_finish_reason(choice["finish_reason"])
        return ev

    def finalize(self) -> list[bytes]:
        if self.finished:
            return []
        self.finished = True
        ev: list[bytes] = []
        self._start(ev)
        self._close(ev)
        for _, slot in sorted(self.tools.items()):
            idx = self.next_index
            self.next_index += 1
            ev.append(sse("content_block_start", {"type": "content_block_start", "index": idx, "content_block": {
                "type": "tool_use", "id": slot["id"] or f"toolu_{uuid.uuid4().hex[:16]}",
                "name": slot["name"], "input": {}}}))
            args = "".join(slot["args"]) or "{}"
            ev.append(sse("content_block_delta", {"type": "content_block_delta", "index": idx,
                                                  "delta": {"type": "input_json_delta", "partial_json": args}}))
            ev.append(sse("content_block_stop", {"type": "content_block_stop", "index": idx}))
        if self.tools:
            self.stop_reason = "tool_use"
        ev.append(sse("message_delta", {"type": "message_delta",
                                        "delta": {"stop_reason": self.stop_reason, "stop_sequence": None},
                                        "usage": self.usage}))
        ev.append(sse("message_stop", {"type": "message_stop"}))
        return ev


def estimate_tokens(payload: dict[str, Any]) -> int:
    """Rough token count for /v1/messages/count_tokens (no tokenizer in the stdlib)."""
    chars = 0
    images = 0

    def walk(o: Any) -> None:
        nonlocal chars, images
        if isinstance(o, str):
            chars += len(o)
        elif isinstance(o, list):
            for i in o:
                walk(i)
        elif isinstance(o, dict):
            if o.get("type") == "image":
                images += 1
                return
            for k, v in o.items():
                chars += len(str(k)) + 2
                walk(v)

    for key in ("system", "messages", "tools"):
        walk(payload.get(key))
    return max(1, int(chars / 3.7) + images * 1200 + len(payload.get("messages", [])) * 4)


def openai_flags(payload: dict[str, Any]) -> tuple[bool, bool]:
    """(is_agent, is_vision) for Chat Completions `messages` or Responses `input` items."""
    vision = False
    items = payload.get("messages")
    if not isinstance(items, list):
        items = payload.get("input") if isinstance(payload.get("input"), list) else []
    # agent only when the request continues a tool loop: the last item is a tool result
    last = items[-1] if items and isinstance(items[-1], dict) else {}
    agent = last.get("role") == "tool" or str(last.get("type") or "").endswith("_call_output")
    for m in items:
        if not isinstance(m, dict):
            continue
        if isinstance(m.get("content"), list) and any(
                isinstance(p, dict) and p.get("type") in ("input_image",) for p in m["content"]):
            vision = True
        if isinstance(m.get("content"), list):
            vision = vision or any(isinstance(p, dict) and p.get("type") == "image_url" for p in m["content"])
    return agent, vision


def is_tool_continuation(msg: dict[str, Any]) -> bool:
    """A user message that returns tool results (Claude Code's own next step, not a new human prompt)."""
    content = msg.get("content")
    return msg.get("role") == "user" and isinstance(content, list) and any(
        isinstance(b, dict) and b.get("type") == "tool_result" for b in content)


def anthropic_flags(payload: dict[str, Any]) -> tuple[bool, bool]:
    """(is_agent, is_vision) for an Anthropic request.

    is_agent is True only when the last message is a user turn that carries tool_result blocks, i.e. Claude Code
    continuing its own tool loop. A prompt the person typed (even deep in a conversation) stays "user", so
    Copilot bills it as the prompt it is."""
    vision = False
    msgs = [m for m in payload.get("messages", []) if isinstance(m, dict)]
    agent = is_tool_continuation(msgs[-1]) if msgs else False
    for m in msgs:
        content = m.get("content")
        if isinstance(content, list):
            for b in content:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "tool_result":
                    inner = b.get("content")
                    if isinstance(inner, list) and any(isinstance(i, dict) and i.get("type") == "image" for i in inner):
                        vision = True
                elif b.get("type") == "image":
                    vision = True
    return agent, vision


class UsageLog:
    """Per-request token accounting: optional JSONL file plus running totals for GET /stats."""

    FIELDS = ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens")

    def __init__(self, path: Optional[str]):
        self.path = Path(path).expanduser() if path else None
        self._lock = threading.Lock()
        self.started = time.time()
        self.totals: dict[str, Any] = {"requests": 0, "errors": 0, **{f: 0 for f in self.FIELDS},
                                       "by_route": {}, "by_model": {}}

    def record(self, **row: Any) -> None:
        raw = row.get("usage") if isinstance(row.get("usage"), dict) else {}
        usage = {}
        for f in self.FIELDS:
            try:
                usage[f] = max(0, int(raw.get(f) or 0))
            except (TypeError, ValueError):
                usage[f] = 0
        row = {"ts": round(time.time(), 3), **row, "usage": usage}
        with self._lock:
            t = self.totals
            t["requests"] += 1
            if int(row.get("status") or 0) >= 400:
                t["errors"] += 1
            for f in self.FIELDS:
                t[f] += int(usage.get(f) or 0)
            for key, name in (("by_route", row.get("route")), ("by_model", row.get("model"))):
                if name:
                    b = t[key].setdefault(name, {"requests": 0, **{f: 0 for f in self.FIELDS}})
                    b["requests"] += 1
                    for f in self.FIELDS:
                        b[f] += int(usage.get(f) or 0)
            if self.path:
                try:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    with self.path.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                except OSError as exc:
                    LOG.warning("usage log write failed: %s", exc)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            t = json.loads(json.dumps(self.totals))
        prompt = t["input_tokens"] + t["cache_read_input_tokens"] + t["cache_creation_input_tokens"]
        t["prompt_tokens_total"] = prompt
        t["cache_hit_ratio"] = round(t["cache_read_input_tokens"] / prompt, 4) if prompt else 0.0
        t["uptime_s"] = int(time.time() - self.started)
        return t


ERROR_TYPES = {400: "invalid_request_error", 401: "authentication_error", 403: "permission_error",
               404: "not_found_error", 413: "request_too_large", 429: "rate_limit_error", 529: "overloaded_error"}


def anthropic_error(status: int, message: str) -> dict[str, Any]:
    return {"type": "error", "error": {"type": ERROR_TYPES.get(status, "api_error"), "message": message}}


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------


class ProxyServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: tuple[str, int], tm: TokenManager, catalog: ModelCatalog,
                 guard: RequestGuard, show_token: bool, mode: str = "auto", count_mode: str = "auto",
                 usage: Optional[UsageLog] = None):
        self.tm = tm
        self.catalog = catalog
        self.guard = guard
        self.show_token = show_token
        self.mode = mode              # auto | native | translate
        self.count_mode = count_mode  # auto | native | estimate
        self.usage = usage or UsageLog(None)
        self.cors_origins: set[str] = set()
        self.allowed_hosts = {"127.0.0.1", "localhost", "[::1]"} | {h for h in [host_name(str(addr[0]))] if h}
        # binding to all interfaces is an explicit choice to accept any Host
        self.check_host = addr[0] not in ("0.0.0.0", "::", "")
        super().__init__(addr, Handler)

    def use_native(self, model: str) -> bool:
        if self.mode == "translate":
            return False
        if self.mode == "native":
            return True
        return self.catalog.supports_endpoint(model, "/v1/messages")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: ProxyServer

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.info("%s %s", self.address_string(), fmt % args)

    # -- responses ---------------------------------------------------------

    def _cors(self) -> None:
        # No CORS by default: a browser page must not be able to spend the Copilot account.
        origin = self.headers.get("Origin")
        allowed = self.server.cors_origins
        if origin and ("*" in allowed or origin in allowed):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers",
                             "Content-Type, Authorization, x-api-key, anthropic-version, anthropic-beta")

    def _request_allowed(self) -> bool:
        """Block DNS rebinding (unexpected Host) and cross-site browser requests (Origin not allowed)."""
        raw = self.headers.get("Host") or ""
        name = host_name(raw)
        reason = None
        if self.server.check_host and (name is None or name not in self.server.allowed_hosts):
            reason = f"Host '{raw}' not allowed (use --allow-host)"
        origin = self.headers.get("Origin")
        allowed = self.server.cors_origins
        if reason is None and origin and not ("*" in allowed or origin in allowed):
            reason = f"Origin '{origin}' not allowed (use --cors-origin)"
        if reason is None:
            return True
        self.close_connection = True  # the request body is not read, so the connection cannot be reused
        self._send_json(403, anthropic_error(403, reason), extra={"Connection": "close"})
        return False

    def _send_raw(self, status: int, body: bytes, ctype: str = "application/json; charset=utf-8",
                  extra: Optional[dict[str, str]] = None) -> None:
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            if v:
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, body: Any, extra: Optional[dict[str, str]] = None) -> None:
        self._send_raw(status, json.dumps(body, ensure_ascii=False).encode("utf-8"), extra=extra)

    @staticmethod
    def _passthrough_headers(resp: http.client.HTTPResponse) -> dict[str, str]:
        """Upstream headers worth keeping on errors: Claude Code backs off on retry-after."""
        return {k: resp.getheader(k) or "" for k in ("retry-after", "x-request-id", "request-id")}

    def _begin_sse(self) -> None:
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        self._streaming = True

    def _chunk(self, data: bytes) -> None:
        if data:
            self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
            self.wfile.flush()

    def _end_chunks(self) -> None:
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def _fail(self, status: int, message: str, anthropic: bool = True,
              extra: Optional[dict[str, str]] = None) -> None:
        """Error before headers -> normal response; after the stream started -> SSE error event."""
        if getattr(self, "_streaming", False):
            try:
                self._chunk(sse("error", anthropic_error(status, message)) if anthropic
                            else f"data: {json.dumps({'error': {'message': message}})}\n\n".encode())
                self._end_chunks()
            except OSError:
                pass
            return
        body = anthropic_error(status, message) if anthropic else {"error": {"message": message}}
        try:
            self._send_json(status, body, extra=extra)
        except OSError:
            pass

    def _read_json(self, anthropic: bool = True) -> Optional[dict[str, Any]]:
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n > 0 else b""
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._fail(400, f"Invalid JSON body: {exc}", anthropic)
            return None
        if not isinstance(body, dict):
            self._fail(400, "JSON body must be an object", anthropic)
            return None
        return body

    def _copilot(self, path: str, payload: dict[str, Any], agent: bool, vision: bool,
                 extra_headers: Optional[dict[str, str]] = None):
        """POST to Copilot. Retries once on 401 (token refresh) and up to RETRIES times on 429/5xx, before any
        byte reaches the client, so Claude Code does not re-send the whole prompt after a long backoff."""
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        refreshed = force = False
        retries = 0
        while True:
            token, base = self.server.tm.get_token(force_refresh=force)
            force = False
            headers = self.server.tm.copilot_headers(token, agent, vision)
            if extra_headers:
                headers.update(extra_headers)
            conn, resp = open_upstream(base, path, "POST", headers, body)
            if resp.status == 401 and not refreshed:
                resp.read()
                conn.close()
                refreshed = force = True
                LOG.warning("Copilot 401; refreshing token and retrying")
                continue
            if resp.status in RETRY_STATUSES and retries < RETRIES:
                wait = _retry_after(resp.getheader("Retry-After"), retries)
                if wait is not None:
                    resp.read()
                    conn.close()
                    retries += 1
                    LOG.warning("Copilot %d; retry %d in %.1fs", resp.status, retries, wait)
                    time.sleep(wait)
                    continue
            return conn, resp

    def _guard(self, summary: str, anthropic: bool) -> bool:
        ok, retry = self.server.guard.check(summary)
        if ok:
            return True
        if retry is not None:
            self._fail(429, f"Local rate limit. Retry after {retry:.1f}s.", anthropic)
        else:
            self._fail(403, "Request rejected by manual approval.", anthropic)
        return False

    # -- verbs ---------------------------------------------------------------

    def do_OPTIONS(self) -> None:
        self._streaming = False
        if not self._request_allowed():
            return
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self) -> None:
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        self._streaming = False
        if not self._request_allowed():
            return
        path = urllib.parse.urlsplit(self.path).path.rstrip("/") or "/"
        if path in ("/", "/health"):
            self._send_json(200, {"status": "ok", "service": "copilot-proxy-stdlib",
                                  "api_base_url": self.server.tm.api_base_url})
        elif path in ("/models", "/v1/models"):
            self._send_json(200, self.server.catalog.get_models_payload())
        elif path == "/usage":
            status, data = self.server.tm.fetch_usage()
            self._send_json(status, data)
        elif path == "/stats":
            self._send_json(200, self.server.usage.snapshot())
        elif path == "/token":
            if not self.server.show_token:
                self._send_json(403, {"error": "Disabled. Start with --show-token to enable."})
                return
            token, base = self.server.tm.get_token()
            self._send_json(200, {"token": token, "api_base_url": base})
        else:
            self._send_json(404, anthropic_error(404, f"Not found: {path}"))

    def do_POST(self) -> None:
        self._streaming = False
        if not self._request_allowed():
            self.close_connection = True  # body not read; do not reuse this connection
            return
        path = urllib.parse.urlsplit(self.path).path.rstrip("/")  # Claude Code adds ?beta=true
        if path in ("/v1/messages", "/messages"):
            self._messages()
        elif path in ("/v1/messages/count_tokens", "/messages/count_tokens"):
            self._count_tokens()
        elif path in ("/v1/chat/completions", "/chat/completions"):
            self._openai_route("/chat/completions")
        elif path in ("/v1/responses", "/responses"):
            self._openai_route("/responses")
        elif path in ("/v1/embeddings", "/embeddings"):
            self._passthrough("/embeddings")
        else:
            n = int(self.headers.get("Content-Length") or 0)
            if n:
                self.rfile.read(n)
            self._send_json(404, anthropic_error(404, f"Not found: {path}"))

    # -- endpoints -------------------------------------------------------------

    def _messages(self) -> None:
        payload = self._read_json()
        if payload is None:
            return
        if not isinstance(payload.get("messages"), list):
            self._fail(400, "messages: must be a list")
            return
        requested = str(payload.get("model", ""))
        cat = self.server.catalog
        model = cat.resolve_model(requested)
        stream = bool(payload.get("stream"))
        if not self._guard(f"POST /v1/messages model={model} stream={stream}", True):
            return
        if self.server.use_native(model) and self._messages_native(payload, requested, model, stream):
            return
        t0 = time.time()
        oai, agent, vision = anthropic_to_openai_request(payload, model, cat.max_output_tokens(model))
        LOG.info("messages: %s -> %s stream=%s initiator=%s vision=%s msgs=%d tools=%d", requested, model,
                 stream, "agent" if agent else "user", vision, len(oai["messages"]), len(oai.get("tools", [])))
        conn = None
        try:
            conn, resp = self._copilot("/chat/completions", oai, agent, vision)
            log = dict(route="translate", model=model, stream=stream, initiator="agent" if agent else "user")
            if resp.status != 200:
                err = resp.read().decode("utf-8", errors="replace")
                LOG.warning("Copilot error %d: %s", resp.status, err[:500])
                self.server.usage.record(**log, status=resp.status, ms=int((time.time() - t0) * 1000))
                self._fail(resp.status, err, extra=self._passthrough_headers(resp))
                return
            if not stream:
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
                out = openai_to_anthropic_response(data, requested or model)
                self.server.usage.record(**log, status=200, usage=out["usage"], ms=int((time.time() - t0) * 1000))
                self._send_json(200, out)
                return
            tr = StreamTranslator(requested or model)
            self._begin_sse()
            while True:
                line = resp.readline()
                if not line:
                    break
                line = line.decode("utf-8", errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                data_str = line[5:].strip()
                if data_str == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                for ev in tr.feed(chunk):
                    self._chunk(ev)
            for ev in tr.finalize():
                self._chunk(ev)
            self._end_chunks()
            self.server.usage.record(**log, status=200, usage=dict(tr.usage), ms=int((time.time() - t0) * 1000))
        except (BrokenPipeError, ConnectionResetError) as exc:
            self._conn_error(exc)
        except Exception as exc:
            LOG.exception("/v1/messages failed")
            self._fail(502, f"Upstream proxy error: {exc}")
        finally:
            if conn is not None:
                conn.close()

    def _conn_error(self, exc: Exception) -> None:
        """A socket error on either side. Never reuse the connection; if nothing was sent yet the client
        still gets a 502 (the write fails quietly if it was the client that went away)."""
        self.close_connection = True
        LOG.info("Connection error: %s", exc)
        if not getattr(self, "_streaming", False):
            self._fail(502, f"Upstream connection error: {exc}")

    def _native_headers(self) -> dict[str, str]:
        h = {"anthropic-version": self.headers.get("anthropic-version") or "2023-06-01"}
        betas = self.headers.get_all("anthropic-beta") or []
        if betas and os.environ.get("COPILOT_PROXY_DROP_BETA") != "1":
            h["anthropic-beta"] = ",".join(b.strip() for b in betas)
        return h

    def _messages_native(self, payload: dict[str, Any], requested: str, model: str, stream: bool) -> bool:
        """Forward the Anthropic request unchanged (model id mapped, max_tokens clamped) to Copilot's own
        /v1/messages. Keeps cache_control breakpoints, thinking blocks and signatures intact.
        Returns False (nothing sent to the client) when Copilot rejects the request shape in auto mode,
        so the caller can fall back to the translated route."""
        t0 = time.time()
        body = dict(payload)
        body["model"] = model
        limit = self.server.catalog.max_output_tokens(model)
        if limit and isinstance(body.get("max_tokens"), int) and body["max_tokens"] > limit:
            body["max_tokens"] = limit
            # Anthropic rejects thinking.budget_tokens >= max_tokens; keep the invariant after clamping
            th = body.get("thinking")
            if isinstance(th, dict) and isinstance(th.get("budget_tokens"), int) and th["budget_tokens"] >= limit:
                if limit <= 2048:
                    body.pop("thinking")  # no room for the 1,024-token minimum budget plus an answer
                else:
                    body["thinking"] = {**th, "budget_tokens": limit - 1024}
        agent, vision = anthropic_flags(body)
        LOG.info("messages(native): %s -> %s stream=%s initiator=%s vision=%s msgs=%d tools=%d", requested, model,
                 stream, "agent" if agent else "user", vision, len(body.get("messages", [])),
                 len(body.get("tools") or []))
        log = dict(route="native", model=model, stream=stream, initiator="agent" if agent else "user")
        conn = None
        try:
            conn, resp = self._copilot("/v1/messages", body, agent, vision, self._native_headers())
            if resp.status != 200 or not stream:
                raw = resp.read()
                usage = _dict_get(_json_or_none(raw), "usage")
                if resp.status != 200:
                    LOG.warning("Copilot native error %d: %s", resp.status, raw[:500])
                if self.server.mode == "auto" and _route_unsupported(resp.status, raw):
                    # only when the endpoint/model itself is refused; a real 400 (context too long, bad
                    # request) goes back to Claude Code instead of re-sending the whole prompt elsewhere
                    LOG.warning("Native route not available (%d); falling back to the translated route", resp.status)
                    return False
                self.server.usage.record(**log, status=resp.status, usage=usage, ms=int((time.time() - t0) * 1000))
                self._send_raw(resp.status, raw, resp.getheader("Content-Type", "application/json; charset=utf-8"),
                               extra=self._passthrough_headers(resp) if resp.status != 200 else None)
                return True
            self._begin_sse()
            usage = {}
            while True:
                line = resp.readline()
                if not line:
                    break
                self._chunk(line)
                if line.startswith(b"data:") and b'"usage"' in line:
                    ev = _json_or_none(line[5:].strip())
                    if ev.get("type") == "message_start":
                        ev = _dict_get(ev, "message")
                    usage.update({k: v for k, v in _dict_get(ev, "usage").items() if isinstance(v, int) and v})
            self._end_chunks()
            self.server.usage.record(**log, status=200, usage=usage, ms=int((time.time() - t0) * 1000))
        except (BrokenPipeError, ConnectionResetError) as exc:
            self._conn_error(exc)
        except Exception as exc:
            LOG.exception("/v1/messages (native) failed")
            self._fail(502, f"Upstream proxy error: {exc}")
        finally:
            if conn is not None:
                conn.close()
        return True

    def _count_tokens(self) -> None:
        payload = self._read_json()
        if payload is None:
            return
        mode = self.server.count_mode
        model = self.server.catalog.resolve_model(str(payload.get("model", "")))
        if mode == "native" or (mode == "auto" and self.server.catalog.supports_endpoint(model, "/v1/messages")):
            body = {k: v for k, v in payload.items() if k in ("model", "system", "messages", "tools", "tool_choice",
                                                               "thinking")}
            body["model"] = model
            conn = None
            try:
                vision = anthropic_flags(body)[1] if isinstance(body.get("messages"), list) else False
                conn, resp = self._copilot("/v1/messages/count_tokens", body, False, vision, self._native_headers())
                raw = resp.read()
                if resp.status == 200:
                    data = json.loads(raw)
                    if isinstance(data.get("input_tokens"), int):
                        self._send_json(200, {"input_tokens": data["input_tokens"]})
                        return
                LOG.warning("native count_tokens failed (%s); using the estimate", resp.status)
            except Exception as exc:
                LOG.warning("native count_tokens error: %s; using the estimate", exc)
            finally:
                if conn is not None:
                    conn.close()
        self._send_json(200, {"input_tokens": estimate_tokens(payload)})

    def _openai_route(self, upstream: str) -> None:
        """OpenAI-format pass-through (/chat/completions, /responses), streaming kept byte for byte."""
        payload = self._read_json(anthropic=False)
        if payload is None:
            return
        if payload.get("model"):
            payload["model"] = self.server.catalog.resolve_model(str(payload["model"]))
        stream = bool(payload.get("stream"))
        agent, vision = openai_flags(payload)
        if not self._guard(f"POST {upstream} model={payload.get('model')} stream={stream}", False):
            return
        t0 = time.time()
        log = dict(route=upstream.strip("/"), model=payload.get("model"), stream=stream,
                   initiator="agent" if agent else "user")
        conn = None
        try:
            conn, resp = self._copilot(upstream, payload, agent, vision)
            if resp.status != 200 or not stream:
                raw = resp.read()
                usage = openai_usage(_dict_get(_json_or_none(raw), "usage"))
                self.server.usage.record(**log, status=resp.status, usage=usage, ms=int((time.time() - t0) * 1000))
                self._send_raw(resp.status, raw, resp.getheader("Content-Type", "application/json; charset=utf-8"),
                               extra=self._passthrough_headers(resp) if resp.status != 200 else None)
                return
            self._begin_sse()
            usage = {}
            while True:
                line = resp.readline()
                if not line:
                    break
                self._chunk(line)
                if line.startswith(b"data:") and b'"usage"' in line:
                    ev = _json_or_none(line[5:].strip())
                    u = _dict_get(ev, "usage") or _dict_get(_dict_get(ev, "response"), "usage")
                    if u:
                        usage = openai_usage(u)  # chat: last chunk; responses: response.completed
            self._end_chunks()
            self.server.usage.record(**log, status=200, usage=usage, ms=int((time.time() - t0) * 1000))
        except (BrokenPipeError, ConnectionResetError) as exc:
            self._conn_error(exc)
        except Exception as exc:
            LOG.exception("%s failed", upstream)
            self._fail(502, str(exc), anthropic=False)
        finally:
            if conn is not None:
                conn.close()

    def _passthrough(self, upstream: str) -> None:
        payload = self._read_json(anthropic=False)
        if payload is None:
            return
        conn = None
        try:
            conn, resp = self._copilot(upstream, payload, False, False)
            self._send_raw(resp.status, resp.read(), resp.getheader("Content-Type", "application/json; charset=utf-8"))
        except Exception as exc:
            self._fail(502, str(exc), anthropic=False)
        finally:
            if conn is not None:
                conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def resolve_github_token(args: argparse.Namespace) -> str:
    """Order: --github-token, token file, Copilot editor login, GH_TOKEN/GITHUB_TOKEN (--use-env-token), device flow."""
    if getattr(args, "github_token", None):
        return args.github_token.strip()
    token_path = Path(args.token_file)
    saved = load_github_token(token_path)
    if saved:
        ok, status = copilot_token_works(saved)
        if ok:
            return saved
        LOG.warning("Saved token in %s rejected (%s)", token_path, status)
    if not getattr(args, "no_copilot_config", False):
        tok = token_from_copilot_config(Path(args.copilot_config_dir))
        if tok:
            LOG.info("Using the existing Copilot editor login from %s", args.copilot_config_dir)
            return tok
    env = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if env and getattr(args, "use_env_token", False):
        return env.strip()
    return run_device_auth_flow(token_path, show_token=getattr(args, "show_token", False))


def claude_code_env(base: str, catalog: ModelCatalog, primary: Optional[str], small: Optional[str],
                    subagent: Optional[str] = None) -> dict[str, str]:
    """Claude Code settings for this proxy. Variable names checked against the Claude Code 2.1.330 binary."""
    sonnet = catalog.resolve_model(primary) if primary else (catalog.newest("sonnet") or PREFERRED_MODELS["sonnet"])
    haiku = catalog.resolve_model(small) if small else (catalog.newest("haiku") or sonnet)
    opus = catalog.newest("opus") or sonnet
    subagent = catalog.resolve_model(subagent) if subagent else None
    env = {
        "ANTHROPIC_BASE_URL": base,
        "ANTHROPIC_AUTH_TOKEN": "dummy",
        "ANTHROPIC_MODEL": sonnet,
        "ANTHROPIC_DEFAULT_SONNET_MODEL": sonnet,
        "ANTHROPIC_DEFAULT_OPUS_MODEL": opus,
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": haiku,
        # Each subagent starts with a fresh user prompt, which Copilot bills at the subagent model's rate
        # (legacy plans: Sonnet 4.6 = 9, Haiku 4.5 = 0.33 premium requests). Override with --subagent-model.
        "CLAUDE_CODE_SUBAGENT_MODEL": subagent or haiku,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    }
    window = catalog.max_prompt_tokens(sonnet)
    if window:
        # compact against Copilot's real prompt limit, not the model's native window
        env["CLAUDE_CODE_AUTO_COMPACT_WINDOW"] = str(window)
    return env


def cmd_auth(args: argparse.Namespace) -> None:
    run_device_auth_flow(Path(args.token_file), show_token=args.show_token)


def cmd_check_usage(args: argparse.Namespace) -> None:
    tm = TokenManager(resolve_github_token(args), account_type=args.account_type)
    status, data = tm.fetch_usage()
    if status != 200:
        print(f"Usage request failed ({status}): {json.dumps(data, indent=2)}", file=sys.stderr)
        sys.exit(1)
    print(f"Copilot plan: {data.get('copilot_plan', 'unknown')}")
    if data.get("quota_reset_date"):
        print(f"Quota reset: {data['quota_reset_date']}")
    snaps = data.get("quota_snapshots")
    if isinstance(snaps, dict):
        print("Quotas:")
        for name, q in snaps.items():
            if not isinstance(q, dict):
                continue
            if q.get("unlimited"):
                print(f"  - {name}: unlimited")
            else:
                print(f"  - {name}: {q.get('remaining', 0)}/{q.get('entitlement', 0)} remaining "
                      f"({float(q.get('percent_remaining', 0)):.1f}%)")
    else:
        print(json.dumps(data, indent=2))


def cmd_start(args: argparse.Namespace) -> None:
    tm = TokenManager(resolve_github_token(args), account_type=args.account_type, show_token=args.show_token)
    tm.start_background_refresh()
    catalog = ModelCatalog(tm, primary=args.model, small=args.small_model)
    catalog.get_models_payload(force=True)
    ids = catalog.chat_model_ids()
    guard = RequestGuard(args.rate_limit, args.wait, args.manual)
    server = ProxyServer((args.host, args.port), tm, catalog, guard, args.show_token,
                         mode=args.mode, count_mode=args.count_tokens, usage=UsageLog(args.usage_log))
    server.cors_origins = set(args.cors_origin or [])
    server.allowed_hosts |= {h for h in (host_name(a) for a in (args.allow_host or [])) if h}
    if not server.check_host:
        LOG.warning("Bound to %s: any Host is accepted and anyone who can reach this port uses your Copilot account",
                    args.host)

    shown = f"[{args.host}]" if ":" in args.host and not args.host.startswith("[") else args.host
    base = f"http://{shown}:{args.port}"
    print(f"\nCopilot proxy listening on {base}")
    print(f"Upstream: {tm.api_base_url}")
    native = [m for m in ids if catalog.supports_endpoint(m, "/v1/messages")]
    print(f"Messages route: {args.mode} (native /v1/messages models: {', '.join(native) or 'none'}); "
          f"count_tokens: {args.count_tokens}" + (f"; usage log: {args.usage_log}" if args.usage_log else ""))
    print(f"Chat models ({len(ids)}): {', '.join(ids)}")
    if args.claude_code:
        env = claude_code_env(base, catalog, args.model, args.small_model, args.subagent_model)
        print("\nRun Claude Code in another terminal:\n  " + " ".join(f"{k}={v}" for k, v in env.items()) + " claude")
        print("\nOr put this in .claude/settings.json:\n" + json.dumps({"env": env}, indent=2))
    print("\nCtrl+C to stop.\n", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        tm.stop()
        server.server_close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="copilot_proxy.py",
                                description="Standard-library proxy: Claude Code CLI -> GitHub Copilot.")
    p.add_argument("--token-file", default=str(default_token_path()),
                   help="GitHub OAuth token file (default: ~/.local/share/copilot-api/github_token)")
    p.add_argument("--copilot-config-dir", default=str(default_copilot_config_dir()),
                   help="Copilot editor login directory to reuse (default: ~/.config/github-copilot)")
    p.add_argument("--no-copilot-config", action="store_true", help="Do not reuse the Copilot editor login")
    p.add_argument("--use-env-token", action="store_true", help="Allow GH_TOKEN/GITHUB_TOKEN as the GitHub token")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    sub = p.add_subparsers(dest="command", required=True)

    def account(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("-a", "--account-type", choices=tuple(COPILOT_API_URLS), default="individual")
        sp.add_argument("-g", "--github-token", default=None, help="GitHub OAuth token (skip login lookup)")
        sp.add_argument("--show-token", action="store_true", help="Log tokens and enable GET /token")

    s = sub.add_parser("start", help="Start the proxy")
    account(s)
    s.add_argument("-p", "--port", type=int, default=DEFAULT_PORT)
    s.add_argument("-H", "--host", default=DEFAULT_HOST)
    s.add_argument("-m", "--model", default=None, help="Force the main model id")
    s.add_argument("-s", "--small-model", default=None, help="Force the small (haiku) model id")
    s.add_argument("-r", "--rate-limit", type=float, default=None, help="Minimum seconds between requests")
    s.add_argument("-w", "--wait", action="store_true", help="Wait instead of returning 429 when rate limited")
    s.add_argument("--manual", action="store_true", help="Ask y/N before forwarding each request")
    s.add_argument("-c", "--claude-code", action="store_true", help="Print the Claude Code environment")
    s.add_argument("--cors-origin", action="append",
                   help="allow browser requests from this Origin (repeatable; '*' = any). Default: none")
    s.add_argument("--allow-host", action="append",
                   help="extra Host header value to accept, e.g. a hostname that points at this machine (repeatable)")
    s.add_argument("--subagent-model", default=None,
                   help="model for Claude Code subagents in the printed env (default: the haiku model)")
    s.add_argument("--mode", choices=("auto", "native", "translate"),
                   default=os.environ.get("COPILOT_PROXY_MODE", "auto"),
                   help="auto: native /v1/messages pass-through for models that list it (keeps prompt caching), "
                        "else translate to /chat/completions")
    s.add_argument("--count-tokens", choices=("auto", "native", "estimate"),
                   default=os.environ.get("COPILOT_PROXY_COUNT_TOKENS", "auto"),
                   help="auto: exact count from Copilot when the model supports it, else a local estimate")
    s.add_argument("--usage-log", default=os.environ.get("COPILOT_PROXY_USAGE_LOG"),
                   help="append one JSON line per request (tokens, cache hits, route, ms) to this file")
    s.set_defaults(func=cmd_start)

    a = sub.add_parser("auth", help="Run the GitHub device login and save the token")
    a.add_argument("--show-token", action="store_true")
    a.set_defaults(func=cmd_auth)

    u = sub.add_parser("check-usage", help="Show Copilot plan and quotas")
    account(u)
    u.set_defaults(func=cmd_check_usage)
    return p


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    args.func(args)


if __name__ == "__main__":
    main()
