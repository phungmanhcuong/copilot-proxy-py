#!/usr/bin/env python3
"""
copilot_proxy.py - single-file, standard-library-only proxy that lets the Claude Code CLI
(Anthropic Messages API) and OpenAI clients use a GitHub Copilot subscription.

Python port of the design of github.com/ericc-ch/copilot-api, with these additions:
  * Claude models go through Copilot's native /v1/messages (prompt cache kept), with exact count_tokens and
    stepped server-side context editing; other models are translated to /chat/completions;
  * connections to Copilot are kept open and reused, so a request does not pay a new TLS handshake;
  * a native request is fitted to the model Copilot serves (output limit, thinking mode, effort level), so that
    Claude Code's settings for another model are not rejected;
  * every failure gets an answer: an HTTP error before the stream starts, an Anthropic `event: error` after;
  * on the translated route tool calls are buffered per index and emitted as complete blocks, so argument
    deltas can never land on a closed block;
  * only the three Claude 5.5 models by default, every request mapped by family and printed as the
    Claude Code variables (ANTHROPIC_DEFAULT_*_MODEL); with --any-model the live /models catalog, where an
    unknown model re-fetches the catalog and falls back to a real chat model;
  * an existing Copilot editor login (~/.config/github-copilot/apps.json) is reused;
  * `--manual` approval prompts are serialised with one lock;
  * Unix-socket listener (ANTHROPIC_UNIX_SOCKET), optional client key, Claude Code sandbox settings, a macOS
    sandbox-exec mode for the proxy itself, and HTTPS_PROXY support.

Requires Python 3.9+. No third-party packages.
"""

from __future__ import annotations

import argparse
import base64
import hmac
import http.client
import json
import logging
import math
import os
import re
import secrets
import select
import shlex
import signal
import socket
import socketserver
import ssl
import stat
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

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

# Client identity and API versions of the current stable GitHub Copilot client: VS Code 1.147.0 with its bundled
# Copilot Chat 0.75.0 (microsoft/vscode tag 1.147.0, extensions/copilot; the standalone vscode-copilot-chat repo
# was archived in May 2026). Copilot API calls use 2026-08-01: networking.ts has 2026-01-09, but the
# @vscode/copilot-api 0.5.2 package the extension depends on overwrites it in _mixinHeaders for every Copilot API
# request. The token exchange and quota calls on api.github.com still use 2025-04-01 (copilotTokenManager.ts,
# chatQuotaServiceImpl.ts). Override with env when a newer client ships.
VSCODE_VERSION = os.environ.get("COPILOT_PROXY_VSCODE_VERSION", "1.147.0")
COPILOT_CHAT_VERSION = os.environ.get("COPILOT_PROXY_CHAT_VERSION", "0.75.0")
EDITOR_VERSION = f"vscode/{VSCODE_VERSION}"
EDITOR_PLUGIN_VERSION = f"copilot-chat/{COPILOT_CHAT_VERSION}"
COPILOT_USER_AGENT = f"GitHubCopilotChat/{COPILOT_CHAT_VERSION}"
GITHUB_API_VERSION = os.environ.get("COPILOT_PROXY_GITHUB_API_VERSION", "2025-04-01")    # api.github.com
COPILOT_API_VERSION = os.environ.get("COPILOT_PROXY_COPILOT_API_VERSION", "2026-08-01")  # api.*.githubcopilot.com
COPILOT_INTENT = "conversation-panel"

# The Claude models this proxy offers and uses (Claude 5.5 family). By default every request is mapped to one of
# these three by family (opus / sonnet / haiku; anything else -> sonnet) and /v1/models lists only them.
# --any-model (COPILOT_PROXY_ANY_MODEL=1) restores the full Copilot catalog, where these are the preferred defaults
# and the newest listed version of the family is the fallback. Override with COPILOT_PROXY_DEFAULT_{SONNET,OPUS,HAIKU}.
PREFERRED_MODELS = {
    "sonnet": os.environ.get("COPILOT_PROXY_DEFAULT_SONNET", "claude-sonnet-5.5"),
    "opus": os.environ.get("COPILOT_PROXY_DEFAULT_OPUS", "claude-opus-5.5"),
    "haiku": os.environ.get("COPILOT_PROXY_DEFAULT_HAIKU", "claude-haiku-5.5"),
}


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.environ.get(name, default))))
    except ValueError:
        return default


DEFAULT_PORT = 4141
DEFAULT_HOST = "127.0.0.1"
# Upstream 429/5xx retries before streaming. Default 0: Claude Code's SDK already retries on Retry-After (which
# the proxy forwards), and a rejected 429/5xx costs no tokens, so proxy-side retries only help other clients.
RETRIES = _env_int("COPILOT_PROXY_RETRIES", 0, 0, 5)
RETRY_STATUSES = (429, 500, 502, 503, 504, 529)
MAX_RETRY_WAIT = 20.0  # longer Retry-After values go back to the client instead of blocking here
CONNECT_TIMEOUT = 15   # seconds to reach GitHub or Copilot (TCP + TLS), and for each short GitHub API call
REQUEST_TIMEOUT = 600  # longest silence accepted from Copilot while it answers
MAX_BODY = 64 * 1024 * 1024  # largest accepted request body (Claude Code sends images base64-encoded)
# Idle connections to Copilot kept for the next request. Measured 2026-11: a new TLS connection to
# api.individual.githubcopilot.com took 524 ms (median of 5); Copilot kept an idle connection open for 90 s and
# had closed it by 120 s.
POOL_MAX_IDLE = 8
POOL_IDLE_SECONDS = 90.0
JSON_TYPE = "application/json; charset=utf-8"

# Server-side context editing on the native Claude route, with the values the official Copilot client sends
# (microsoft/vscode extensions/copilot platform/networking/common/anthropic.ts buildContextManagement): once a
# request is over CLEAR_TRIGGER input tokens, Copilot replaces all but the last CLEAR_KEEP tool results with a
# placeholder. Only added when the client sent no `context_management` of its own.
CONTEXT_EDIT_MODES = ("off", "tools", "thinking", "both")
CONTEXT_MANAGEMENT_BETA = "context-management-2025-06-27"
CLEAR_TRIGGER = _env_int("COPILOT_PROXY_CLEAR_TRIGGER", 100000, 1000, 10_000_000)
CLEAR_KEEP = _env_int("COPILOT_PROXY_CLEAR_KEEP", 3, 0, 1000)
CLEAR_STEP = _env_int("COPILOT_PROXY_CLEAR_STEP", 10, 1, 1000)

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
# JSON and HTTP helpers
# ---------------------------------------------------------------------------

_SSL = ssl.create_default_context()


class UpstreamError(RuntimeError):
    """GitHub or Copilot could not be reached, refused the login, or sent an unreadable answer (client: 502)."""


def _dumps(obj: Any) -> bytes:
    """Compact UTF-8 JSON. Text cut in the middle of an emoji holds half a surrogate pair, which UTF-8 cannot
    encode; such a document is written with \\u escapes instead, the way it arrived."""
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except UnicodeEncodeError:
        return json.dumps(obj, separators=(",", ":")).encode("ascii")


def _json_object(raw: Any) -> dict[str, Any]:
    """Parse JSON bytes/str; anything that is not a JSON object becomes {}."""
    try:
        v = json.loads(raw) if raw else {}
    except (ValueError, TypeError, RecursionError):
        return {}
    return v if isinstance(v, dict) else {}


def _dict_get(d: Any, key: str) -> dict[str, Any]:
    v = d.get(key) if isinstance(d, dict) else None
    return v if isinstance(v, dict) else {}


def _dicts(items: Any) -> list[dict[str, Any]]:
    """The objects in a JSON list (content blocks, messages, choices). A string, null or any other value has
    none, so callers can loop over client or Copilot data without checking its shape first."""
    return [i for i in items if isinstance(i, dict)] if isinstance(items, list) else []


def http_json_request(
    method: str,
    url: str,
    headers: Optional[dict[str, str]] = None,
    payload: Optional[dict[str, Any]] = None,
    timeout: float = CONNECT_TIMEOUT,
) -> tuple[int, dict[str, Any]]:
    """One short JSON request to GitHub or Copilot via urllib (which honours HTTPS_PROXY). Never raises: returns
    (status, body). Status 0 means no answer (network error). The body is always a dict; an answer that is not
    a JSON object is returned as {"error": text}."""
    data = _dumps(payload) if payload is not None else None
    req_headers = {"Accept": "application/json", **({"Content-Type": "application/json"} if data else {}),
                   **(headers or {})}
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL) as resp:
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read()
    except (OSError, http.client.HTTPException) as exc:  # no route, DNS, TLS, timeout
        return 0, {"error": f"{type(exc).__name__}: {exc}"}
    body = _json_object(raw)
    if not body and raw.strip() not in (b"", b"{}"):
        body = {"error": raw.decode("utf-8", errors="replace")[:2000]}
    return status, body


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


def _route_unsupported(status: int, raw: bytes) -> bool:
    """Copilot refused the native endpoint or model itself (not the request content)."""
    if status in (404, 405, 415):
        return True
    if status in (400, 422):
        text = raw[:2000].decode("utf-8", errors="replace").lower()
        if "model_not_supported" in text or "unknown endpoint" in text:
            return True
        # only a refusal of the model or endpoint; a rejected field, beta or strategy is the request's own error
        # and must not silently move the whole prompt to the translated route (losing the prompt cache)
        about_route = ("model" in text or "endpoint" in text) and not any(
            s in text for s in ("context_management", "context management", "context-management", "beta", "strategy",
                                "field", "inputs", "thinking", "tool", "parameter", "image", "cache"))
        return about_route and ("not supported" in text or "unsupported" in text)
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


def upstream_proxy(scheme: str, host: str) -> Optional[tuple[str, int, dict[str, str]]]:
    """(proxy host, port, Proxy-Authorization header) from the environment, or None."""
    if not host or urllib.request.proxy_bypass(host):
        return None
    proxies = urllib.request.getproxies()
    url = proxies.get(scheme) or proxies.get("all")
    if not url:
        return None
    p = urllib.parse.urlsplit(url if "://" in url else "http://" + url)
    if p.scheme not in ("http", "") or not p.hostname:
        # fail closed: going direct could bypass a required egress proxy
        raise OSError(f"Unsupported {p.scheme}:// proxy for {scheme} (only http:// proxies)")
    try:
        port = p.port or 8080
    except ValueError:
        raise OSError(f"Invalid port in {scheme} proxy setting")
    auth = {}
    if p.username is not None:
        cred = f"{urllib.parse.unquote(p.username)}:{urllib.parse.unquote(p.password or '')}"
        auth["Proxy-Authorization"] = "Basic " + base64.b64encode(cred.encode()).decode()
    return p.hostname, port, auth


# ---------------------------------------------------------------------------
# Connections to Copilot
# ---------------------------------------------------------------------------


class _Link:
    """An open connection to Copilot and what a request on it needs: the path prefix and, through a plain-HTTP
    proxy, the proxy credentials."""

    __slots__ = ("http", "prefix", "headers", "idle_since")

    def __init__(self, conn: http.client.HTTPConnection, prefix: str, headers: dict[str, str]):
        self.http, self.prefix, self.headers, self.idle_since = conn, prefix, headers, 0.0


def _connect(base_url: str) -> _Link:
    """New connection to the upstream. Honours HTTPS_PROXY / HTTP_PROXY / NO_PROXY like urllib does (HTTPS
    through an HTTP CONNECT tunnel)."""
    url = urllib.parse.urlsplit(base_url)
    host, tls = url.hostname or "", url.scheme == "https"
    port = url.port or (443 if tls else 80)
    prefix, extra = url.path.rstrip("/"), {}
    proxy = upstream_proxy(url.scheme, host)
    if tls:
        conn: http.client.HTTPConnection = http.client.HTTPSConnection(
            *(proxy[:2] if proxy else (host, port)), timeout=CONNECT_TIMEOUT, context=_SSL)
        if proxy:
            conn.set_tunnel(host, port, headers=proxy[2])
    elif proxy:  # plain HTTP through a proxy: the full URL and the credentials go on every request
        conn = http.client.HTTPConnection(proxy[0], proxy[1], timeout=CONNECT_TIMEOUT)
        prefix, extra = f"{url.scheme}://{url.netloc}{prefix}", proxy[2]
    else:
        conn = http.client.HTTPConnection(host, port, timeout=CONNECT_TIMEOUT)
    conn.auto_open = 0  # a closed link is never silently reopened (it would keep the short connect timeout)
    try:
        conn.connect()
        conn.sock.settimeout(REQUEST_TIMEOUT)
    except BaseException:
        conn.close()
        raise
    return _Link(conn, prefix, extra)


def _looks_open(conn: http.client.HTTPConnection) -> bool:
    """False when the other side has closed an idle connection (its socket turned readable)."""
    if conn.sock is None:
        return False
    try:
        return not select.select([conn.sock], [], [], 0)[0]
    except (OSError, ValueError):
        return False


class UpstreamResponse:
    """One answer from Copilot. Read it whole with body() (kept, so it can be read again) or as it arrives with
    lines() / blocks(). Once it is read to the end its connection goes back to the pool; close() ends an answer
    that was not read to the end by closing the connection."""

    def __init__(self, pool: UpstreamPool, key: str, link: _Link, resp: http.client.HTTPResponse):
        self.status = resp.status
        self._pool, self._key, self._resp = pool, key, resp
        self._link: Optional[_Link] = link
        self._body: Optional[bytes] = None
        self._complete = False

    def header(self, name: str, default: str = "") -> str:
        return self._resp.getheader(name) or default

    def body(self) -> bytes:
        if self._body is None:
            self._body = self._resp.read()
            self._complete = True
            self.close()
        return self._body

    def lines(self) -> Iterator[bytes]:
        return self._until_end(self._resp.readline)

    def blocks(self) -> Iterator[bytes]:
        """The body in the pieces it arrives in (one socket read each), for forwarding without delay."""
        return self._until_end(lambda: self._resp.read1(65536))

    def _until_end(self, read: Callable[[], bytes]) -> Iterator[bytes]:
        while True:
            data = read()
            if not data:
                break
            yield data
        self._resp.read()  # nothing is left; marks the response as finished on every Python version
        self._complete = True
        self.close()

    def close(self) -> None:
        link, self._link = self._link, None
        if link is None:
            return
        if self._complete and not self._resp.will_close:
            self._pool.give(self._key, link)
        else:
            link.http.close()


class UpstreamPool:
    """Idle keep-alive connections to Copilot, per base URL, so that only the first request pays for TCP + TLS."""

    # what a request on a kept connection raises when Copilot closed it in the meantime
    STALE = (ConnectionError, http.client.BadStatusLine, ssl.SSLError)

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._idle: dict[str, list[_Link]] = {}

    def post(self, base_url: str, path: str, headers: dict[str, str], body: bytes) -> UpstreamResponse:
        link = self._take(base_url)
        reused = link is not None
        while True:
            link = link or _connect(base_url)
            try:
                link.http.request("POST", link.prefix + path, body=body, headers={**headers, **link.headers})
                return UpstreamResponse(self, base_url, link, link.http.getresponse())
            except BaseException as exc:
                link.http.close()
                # Copilot can close a kept connection just as the request goes out: once more, on a new one
                if not (reused and isinstance(exc, self.STALE)):
                    raise
                link, reused = None, False

    def _take(self, key: str) -> Optional[_Link]:
        while True:
            with self._lock:
                links = self._idle.get(key)
                if not links:
                    return None
                link = links.pop()  # the newest; older ones age out
            if time.monotonic() - link.idle_since < POOL_IDLE_SECONDS and _looks_open(link.http):
                return link
            link.http.close()

    def give(self, key: str, link: _Link) -> None:
        link.idle_since = time.monotonic()
        with self._lock:
            links = self._idle.setdefault(key, [])
            links.append(link)
            dropped = links[:-POOL_MAX_IDLE]
            del links[:-POOL_MAX_IDLE]
        for old in dropped:
            old.http.close()

    def clear(self) -> None:
        with self._lock:
            links = [link for group in self._idle.values() for link in group]
            self._idle.clear()
        for link in links:
            link.http.close()


UPSTREAM = UpstreamPool()


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


def private_dir(path: Path) -> None:
    """Create a directory as 0700; warn when an existing one is writable by others."""
    if not path.exists():
        try:
            path.mkdir(parents=True, mode=0o700)
            os.chmod(path, 0o700)
        except FileExistsError:
            pass
        if not path.is_dir():
            raise SystemExit(f"{path} is not a directory")
    elif os.name != "nt" and path.stat().st_mode & 0o022:
        LOG.warning("%s is writable by other users; its files could be replaced", path)


def save_github_token(path: Path, token: str) -> None:
    """Atomic write, created as 0600 (never briefly readable by others)."""
    private_dir(path.parent)
    fd, name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")  # 0600
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(token.strip() + "\n")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def copilot_token_works(github_token: str) -> tuple[bool, int]:
    """Whether GitHub exchanges this login for a Copilot token. Raises UpstreamError when GitHub cannot be
    reached, so that an outage is not taken for a rejected login."""
    status, data = http_json_request(
        "GET", f"{GITHUB_API_BASE_URL}/copilot_internal/v2/token", github_headers(github_token))
    if status == 0:
        raise UpstreamError(f"Cannot reach {GITHUB_API_BASE_URL}: {data.get('error')}")
    return status == 200 and "token" in data, status


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
    status, data = http_json_request(
        "POST",
        f"{GITHUB_BASE_URL}/login/device/code",
        headers=github_headers(),
        payload={"client_id": GITHUB_CLIENT_ID, "scope": "read:user"},
    )
    if status != 200 or "device_code" not in data:
        raise UpstreamError(f"GitHub device flow start failed ({status}): {data}")

    interval = int(data.get("interval", 5))
    expires_at = time.time() + int(data.get("expires_in", 900))
    print("\nGitHub Copilot login")
    print(f"1. Open: {data['verification_uri']}")
    print(f"2. Code: {data['user_code']}")
    print("Waiting for authorization...\n", flush=True)

    while time.time() < expires_at:
        time.sleep(interval)
        status, p = http_json_request(
            "POST",
            f"{GITHUB_BASE_URL}/login/oauth/access_token",
            payload={
                "client_id": GITHUB_CLIENT_ID,
                "device_code": data["device_code"],
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            },
        )
        if p.get("access_token"):
            save_github_token(token_path, p["access_token"])
            print(f"Login succeeded; token saved to {token_path}")
            if show_token:
                print(f"GitHub token: {p['access_token']}")
            return p["access_token"]
        if p.get("error") == "slow_down":
            interval += 5
        elif status != 0 and p.get("error") != "authorization_pending":  # a network error waits for the next poll
            raise UpstreamError(f"GitHub device flow failed: {p}")
    raise UpstreamError("GitHub device authorization timed out.")


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
        return self._base_url

    def get_token(self, force_refresh: bool = False) -> tuple[str, str]:
        """(Copilot token, API base URL). Raises UpstreamError when the token exchange fails."""
        with self._lock:
            if not force_refresh and self._token and time.time() < self._expires_at - 60:
                return self._token, self._base_url
            return self._refresh_locked()

    def _refresh_locked(self) -> tuple[str, str]:
        status, data = http_json_request(
            "GET", f"{GITHUB_API_BASE_URL}/copilot_internal/v2/token", github_headers(self.github_token))
        if status != 200 or not isinstance(data.get("token"), str):
            raise UpstreamError(f"GitHub -> Copilot token exchange failed ({status}): {str(data)[:300]}")
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

    def fetch_usage(self) -> tuple[int, dict[str, Any]]:
        return http_json_request(
            "GET", f"{GITHUB_API_BASE_URL}/copilot_internal/user", github_headers(self.github_token))

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
            "OpenAI-Intent": COPILOT_INTENT,
            "X-Interaction-Type": COPILOT_INTENT,
            "X-GitHub-Api-Version": COPILOT_API_VERSION,
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
    """The optional pause between upstream requests (-r, -w) and the y/N approval of each one (--manual)."""

    def __init__(self, rate_limit_seconds: Optional[float], wait_on_limit: bool, manual_approval: bool):
        self.rate_limit_seconds = rate_limit_seconds or 0.0
        self.wait_on_limit = wait_on_limit
        self.manual_approval = manual_approval
        self._lock = threading.Lock()
        self._next = 0.0  # monotonic time from which the next request may pass

    def check(self, summary: str) -> tuple[bool, Optional[float]]:
        """(allowed, retry_after). retry_after None + not allowed = rejected by the operator."""
        if self.rate_limit_seconds > 0:
            with self._lock:
                wait = self._next - time.monotonic()
                if wait > 0:
                    if not self.wait_on_limit:
                        return False, wait
                    LOG.info("Rate limit: waiting %.1fs", wait)
                    time.sleep(wait)
                self._next = time.monotonic() + self.rate_limit_seconds

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
    """Copilot's model list (GET /models) and the mapping from the model a client asks for to the one used."""

    MAX_AGE = 300.0     # seconds before the list is fetched again
    RETRY_PAUSE = 60.0  # seconds between attempts while the fetch fails

    def __init__(self, tm: TokenManager, primary: Optional[str] = None, small: Optional[str] = None,
                 any_model: bool = False):
        self.tm = tm
        self.only: Optional[dict[str, str]] = None if any_model else dict(PREFERRED_MODELS)
        if self.only:  # -m / -s must name one of the three; map by family
            for name in (primary, small):
                if name and not self.family_id(name):
                    raise SystemExit(f"{name}: only {', '.join(self.only.values())} are enabled (see --any-model)")
            primary, small = self.family_id(primary), self.family_id(small)
        self.primary = primary
        self.small = small
        self._lock = threading.Lock()
        # the three below are replaced as a whole by a fetch, never changed in place, so reads need no lock
        self._payload: dict[str, Any] = {"object": "list", "data": []}
        self._models: dict[str, dict[str, Any]] = {}
        self._chat_ids: list[str] = []
        self._fetched = 0.0
        self._retry_at = 0.0

    def refresh(self, max_age: float = MAX_AGE) -> bool:
        """Fetch the list when the copy is older than max_age seconds (0 = now). A failed fetch is logged, not
        raised, and not repeated for RETRY_PAUSE seconds: the proxy keeps serving with what it has. Returns
        whether a list is loaded."""
        now = time.monotonic()
        with self._lock:
            fresh = self._models and now - self._fetched < max_age
            if fresh or (max_age and now < self._retry_at):
                return bool(self._models)
            self._retry_at = now + self.RETRY_PAUSE  # also keeps parallel requests from all fetching at once
        try:
            token, base = self.tm.get_token()
            status, data = http_json_request("GET", f"{base}/models", self.tm.copilot_headers(token))
            if status == 401:
                token, base = self.tm.get_token(force_refresh=True)
                status, data = http_json_request("GET", f"{base}/models", self.tm.copilot_headers(token))
        except UpstreamError as exc:
            status, data = 0, {"error": str(exc)}
        if status == 200 and isinstance(data.get("data"), list):
            models = {m["id"]: m for m in data["data"] if isinstance(m, dict) and isinstance(m.get("id"), str)}
            chat = sorted(mid for mid, m in models.items()
                          if _dict_get(m, "capabilities").get("type", "chat") == "chat")
            with self._lock:
                self._payload, self._models, self._chat_ids = data, models, chat
                self._fetched, self._retry_at = time.monotonic(), 0.0
            LOG.info("Loaded %d Copilot models", len(models))
        else:
            LOG.warning("Cannot fetch Copilot models (%s): %s", status, str(data)[:300])
        return bool(self._models)

    def get_models_payload(self, force: bool = False) -> dict[str, Any]:
        """The answer to GET /v1/models. Raises UpstreamError when --any-model has no list to show."""
        loaded = self.refresh(0.0 if force else self.MAX_AGE)
        if self.only:
            # only the three enabled models; one Copilot does not list yet gets a minimal entry
            return {**self._payload, "data": [
                self._models.get(mid) or {"id": mid, "object": "model", "name": mid, "vendor": "Anthropic"}
                for mid in self.only.values()]}
        if not loaded:
            raise UpstreamError("Copilot's model list is not available")
        return self._payload

    def family_id(self, name: Optional[str]) -> Optional[str]:
        """The enabled model for a name's family (opus / sonnet / haiku), or None."""
        low = (name or "").lower()
        if self.only:
            if low in self.only.values():
                return low
            for fam in ("opus", "sonnet", "haiku"):
                if fam in low:
                    return self.only[fam]
        return None

    def missing(self) -> list[str]:
        """Enabled models that this account's Copilot catalog does not list."""
        return [m for m in (self.only or {}).values() if m not in self._chat_ids]

    def chat_model_ids(self) -> list[str]:
        return self._chat_ids

    def newest(self, family: str) -> Optional[str]:
        """Default model for a family: the preferred Claude 5.5 id when this account's catalog serves it,
        otherwise the newest version Copilot lists (e.g. legacy annual plans, which get no new models)."""
        if self.only:
            return self.only.get(family)
        ids = [m for m in self._chat_ids if m.startswith("claude") and family in m]
        preferred = PREFERRED_MODELS.get(family)
        if preferred and preferred in ids:
            return preferred
        return max(ids, key=_version_key) if ids else None

    def _limit(self, model_id: str, name: str) -> Optional[int]:
        limit = _dict_get(_dict_get(self._models.get(model_id), "capabilities"), "limits").get(name)
        return int(limit) if isinstance(limit, (int, float)) and limit > 0 else None

    def max_output_tokens(self, model_id: str) -> Optional[int]:
        return self._limit(model_id, "max_output_tokens")

    def max_prompt_tokens(self, model_id: str) -> Optional[int]:
        return self._limit(model_id, "max_prompt_tokens")

    def supports(self, model_id: str) -> dict[str, Any]:
        """What Copilot lists as the model's capabilities (adaptive thinking, thinking budget, reasoning-effort
        levels, ...); {} when the model or its capabilities are not known."""
        return _dict_get(_dict_get(self._models.get(model_id), "capabilities"), "supports")

    def supports_endpoint(self, model_id: str, endpoint: str) -> bool:
        """Whether Copilot lists this endpoint for the model. Loads the list first if the fetch at start failed:
        without it every request would stay on the translated route, without the prompt cache."""
        self.refresh(math.inf)
        return endpoint in (_dict_get(self._models, model_id).get("supported_endpoints") or [])

    def resolve_model(self, requested: str) -> str:
        """Map the model a client asks for (Anthropic ids are dated and hyphenated) to a Copilot id (dotted)."""
        low = (requested or "").lower()
        if self.small and "haiku" in low:
            return self.small
        if self.primary and "haiku" not in low:
            return self.primary
        if self.only:
            return self.family_id(requested) or self.only["sonnet"]

        stripped = re.sub(r"-\d{8}$", "", requested or "").replace("-latest", "")
        stripped = re.sub(r"\[.*\]$", "", stripped)  # e.g. "claude-x[1m]"
        dotted = re.sub(r"-(\d+)-(\d+)", r"-\1.\2", stripped)
        cands = [requested, stripped, dotted]
        m = re.match(r"^claude-(sonnet|opus|haiku)-(\d+)(?:\.(\d+))?$", dotted)
        if m:
            fam, maj, minor = m.groups()
            if minor:
                cands.append(f"claude-{maj}.{minor}-{fam}")
            cands.append(f"claude-{fam}-{maj}")

        def listed() -> Optional[str]:
            ids = set(self._chat_ids)
            return next((c for c in cands if c in ids), None)

        # not listed: load the list if it is missing or older than MAX_AGE (Copilot may have added the model)
        found = listed() or (self.refresh() and listed()) or None
        if found:
            LOG.debug("Model %s -> %s", requested, found)
            return found
        for fam in ("opus", "sonnet", "haiku"):
            if fam in low:
                best = self.newest(fam)
                if best:
                    LOG.info("Model %s not served; using newest %s: %s", requested, fam, best)
                    return best
        fallback = self.newest("sonnet") or (self._chat_ids[0] if self._chat_ids else None)
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
    """An Anthropic image block as an OpenAI image_url part; None for any other block."""
    src = block.get("source")
    if block.get("type") != "image" or not isinstance(src, dict):
        return None
    if src.get("type") == "base64":
        return {"type": "image_url",
                "image_url": {"url": f"data:{src.get('media_type', 'image/png')};base64,{src.get('data', '')}"}}
    if src.get("type") == "url" and src.get("url"):
        return {"type": "image_url", "image_url": {"url": src["url"]}}
    return None


def _assistant_message(content: Any) -> dict[str, Any]:
    """An Anthropic assistant turn as one OpenAI message. thinking / redacted_thinking blocks are dropped: the
    OpenAI schema has no slot for them."""
    if isinstance(content, str):
        return {"role": "assistant", "content": content}
    texts: list[str] = []
    calls: list[dict[str, Any]] = []
    for b in _dicts(content):
        if b.get("type") == "text":
            texts.append(b.get("text", ""))
        elif b.get("type") == "tool_use":
            inp = b.get("input", {})
            calls.append({
                "id": b.get("id") or f"call_{uuid.uuid4().hex[:16]}",
                "type": "function",
                "function": {"name": b.get("name", ""),
                             "arguments": inp if isinstance(inp, str)
                             else json.dumps(inp, ensure_ascii=False, separators=(",", ":"))},
            })
    text = "\n".join(t for t in texts if t)
    msg: dict[str, Any] = {"role": "assistant", "content": text or (None if calls else "")}
    if calls:
        msg["tool_calls"] = calls
    return msg


def _user_messages(content: Any) -> tuple[list[dict[str, Any]], bool]:
    """An Anthropic user turn as OpenAI messages: one `tool` message per tool result (they must directly follow
    the assistant's tool calls), then one user message with the text and images. Second value: has an image."""
    if isinstance(content, str):
        return [{"role": "user", "content": content}], False
    out: list[dict[str, Any]] = []
    parts: list[dict[str, Any]] = []
    for b in _dicts(content):
        kind = b.get("type")
        if kind == "tool_result":
            text = _blocks_text(b.get("content", ""))
            if b.get("is_error") and not text.startswith("Error"):
                text = f"Error: {text}" if text else "Error"
            out.append({"role": "tool", "tool_call_id": b.get("tool_use_id", ""), "content": text})
            # images inside a tool result (e.g. screenshots) go into the user message that follows
            parts += [img for img in map(_image_part, _dicts(b.get("content"))) if img]
        elif kind == "text":
            parts.append({"type": "text", "text": b.get("text", "")})
        elif kind == "image":
            img = _image_part(b)
            if img:
                parts.append(img)
    has_image = any(p["type"] == "image_url" for p in parts)
    if parts:
        out.append({"role": "user", "content": parts if has_image else "\n".join(p["text"] for p in parts)})
    return out, has_image


def _openai_tools(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], Any]:
    """(tools, tool_choice) in OpenAI form; tool_choice None = leave it out."""
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
    choice = _dict_get(payload, "tool_choice")
    kind = choice.get("type")
    if kind in ("auto", "none"):
        return tools, kind
    if kind == "any":
        return tools, "required"
    if kind == "tool" and choice.get("name"):
        return tools, {"type": "function", "function": {"name": choice["name"]}}
    return tools, None


def anthropic_to_openai_request(
    payload: dict[str, Any], model: str, max_out: Optional[int] = None
) -> tuple[dict[str, Any], bool, bool]:
    """An Anthropic Messages request as a Chat Completions request: (payload, is_agent_initiator, is_vision)."""
    msgs: list[dict[str, Any]] = []
    system = payload.get("system")
    if isinstance(system, list):
        system = "\n\n".join(b["text"] for b in _dicts(system) if b.get("type") == "text" and b.get("text"))
    if isinstance(system, str) and system:
        msgs.append({"role": "system", "content": system})

    is_vision = False
    for msg in payload.get("messages", []):
        if msg.get("role") == "assistant":
            msgs.append(_assistant_message(msg.get("content", "")))
        else:
            turn, has_image = _user_messages(msg.get("content", ""))
            msgs += turn
            is_vision = is_vision or has_image

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
    tools, choice = _openai_tools(payload)
    if tools:
        out["tools"] = tools
        if choice is not None:
            out["tool_choice"] = choice
    return out, anthropic_flags(payload)[0], is_vision  # one initiator rule for both routes


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
    cached = int(_dict_get(usage, "prompt_tokens_details").get("cached_tokens") or 0)
    out = {"input_tokens": max(0, prompt - cached), "output_tokens": int(usage.get("completion_tokens") or 0)}
    if cached:
        out["cache_read_input_tokens"] = cached
    return out


def openai_usage(u: dict[str, Any]) -> dict[str, int]:
    """Chat Completions or Responses usage -> Anthropic-style fields (input excludes cached tokens)."""
    if not u:
        return {}
    try:
        if "prompt_tokens" in u:
            return anthropic_usage(u)
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
    blocks: list[dict[str, Any]] = []
    finish = None
    # Copilot can split one answer over several choices (text in one, tool calls in another)
    for ch in resp.get("choices") or []:
        m = _dict_get(ch, "message")
        finish = ch.get("finish_reason") or finish
        reasoning = m.get("reasoning_content") or m.get("reasoning_text")
        if isinstance(reasoning, str) and reasoning:
            blocks.append({"type": "thinking", "thinking": reasoning, "signature": ""})
        if isinstance(m.get("content"), str) and m["content"]:
            blocks.append({"type": "text", "text": m["content"]})
        for tc in _dicts(m.get("tool_calls")):
            fn = _dict_get(tc, "function")
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
    return b"event: " + event.encode() + b"\ndata: " + _dumps(data) + b"\n\n"


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
        self.finish_seen = False  # Copilot sent a finish_reason: the answer is complete
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
            self.usage.update({k: v for k, v in anthropic_usage(chunk["usage"]).items() if v})
        for choice in _dicts(chunk.get("choices")):
            self._start(ev)
            d = _dict_get(choice, "delta")
            r = d.get("reasoning_content") or d.get("reasoning_text")
            if isinstance(r, str) and r:
                self._delta(ev, "thinking", {"type": "thinking_delta", "thinking": r})
            if isinstance(d.get("content"), str) and d["content"]:
                self._delta(ev, "text", {"type": "text_delta", "text": d["content"]})
            for tc in _dicts(d.get("tool_calls")):
                index = tc.get("index")
                slot = self.tools.setdefault(index if isinstance(index, int) else 0,
                                             {"id": None, "name": "", "args": []})
                fn = _dict_get(tc, "function")
                if tc.get("id"):
                    slot["id"] = tc["id"]
                if fn.get("name"):
                    slot["name"] = slot["name"] or fn["name"]
                if isinstance(fn.get("arguments"), str):
                    slot["args"].append(fn["arguments"])
            if choice.get("finish_reason"):
                self.finish_seen = True
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


# ---------------------------------------------------------------------------
# Request facts: token estimate, initiator, vision, context editing
# ---------------------------------------------------------------------------


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
    items = payload.get("messages")
    if not isinstance(items, list):
        items = payload.get("input")
    items = _dicts(items)
    # agent only when the request continues a tool loop: the last item is a tool result
    last = items[-1] if items else {}
    agent = last.get("role") == "tool" or str(last.get("type") or "").endswith("_call_output")
    vision = any(p.get("type") in ("image_url", "input_image") for m in items for p in _dicts(m.get("content")))
    return agent, vision


def is_tool_continuation(msg: dict[str, Any]) -> bool:
    """A user message that returns tool results (Claude Code's own next step, not a new human prompt)."""
    return msg.get("role") == "user" and any(b.get("type") == "tool_result" for b in _dicts(msg.get("content")))


def anthropic_flags(payload: dict[str, Any]) -> tuple[bool, bool]:
    """(is_agent, is_vision) for an Anthropic request.

    is_agent is True only when the last message is a user turn that carries tool_result blocks, i.e. Claude Code
    continuing its own tool loop. A prompt the person typed (even deep in a conversation) stays "user", so
    Copilot bills it as the prompt it is."""
    msgs = _dicts(payload.get("messages"))
    # the last turn of the conversation decides; a per-turn control message after it (role "system") does not
    turns = [m for m in msgs if m.get("role") in ("user", "assistant")]
    agent = bool(turns) and is_tool_continuation(turns[-1])

    def has_image(blocks: Any) -> bool:
        return any(b.get("type") == "image" for b in _dicts(blocks))

    vision = any(has_image(m.get("content")) or any(has_image(b.get("content")) for b in _dicts(m.get("content"))
                                                    if b.get("type") == "tool_result") for m in msgs)
    return agent, vision


def clamp_output(body: dict[str, Any], limit: Optional[int]) -> None:
    """Keep max_tokens inside the model's output limit, so the request is not rejected and sent again."""
    if not limit or not isinstance(body.get("max_tokens"), int) or body["max_tokens"] <= limit:
        return
    body["max_tokens"] = limit
    # Anthropic rejects thinking.budget_tokens >= max_tokens; keep the invariant after clamping
    th = body.get("thinking")
    if isinstance(th, dict) and isinstance(th.get("budget_tokens"), int) and th["budget_tokens"] >= limit:
        if limit <= 2048:
            del body["thinking"]  # no room for the 1,024-token minimum budget plus an answer
        else:
            body["thinking"] = {**th, "budget_tokens": limit - 1024}


EFFORT_ORDER = ("low", "medium", "high", "xhigh", "max")


def _fit_effort(config: Any, levels: list[str]) -> Any:
    """An output_config with its effort fitted to the levels the model has: the nearest one not above the level
    asked for, else the lowest; no effort for a model without any. The same object when nothing changes."""
    asked = config.get("effort") if isinstance(config, dict) else None
    if asked in levels or asked not in EFFORT_ORDER:
        return config
    fitted = {k: v for k, v in config.items() if k != "effort"}
    usable = [x for x in EFFORT_ORDER if x in levels]
    if usable:
        below = [x for x in usable if EFFORT_ORDER.index(x) <= EFFORT_ORDER.index(asked)]
        fitted["effort"] = below[-1] if below else usable[0]
    return fitted


def fit_to_model(body: dict[str, Any], supports: dict[str, Any]) -> bool:
    """Fit a native request to what Copilot's model list says the model can do (`supports`). Claude Code sends
    the settings of the model it was told it has, and the proxy may have mapped that to another one:
    - adaptive thinking becomes a fixed thinking budget on a model without adaptive thinking;
    - output_config.effort becomes a level the model has, or is removed on a model without reasoning effort.
      The same for the per-turn setting, which Claude Code sends as a last message
      {"role": "system", "content": [], "output_config": {...}}.
    Without this Copilot answers 400 (measured: Claude Code 2.1.332, claude-haiku-4.5). Returns True when a
    per-turn message was removed; the caller then drops the per-turn beta too. Nothing is changed when the
    model's capabilities are not known."""
    if not supports:
        return False
    th = body.get("thinking")
    budget = supports.get("max_thinking_budget")
    if isinstance(th, dict) and th.get("type") == "adaptive" and not supports.get("adaptive_thinking") \
            and isinstance(budget, int) and isinstance(body.get("max_tokens"), int):
        budget = min(budget, body["max_tokens"] - 1024)  # the same room for the answer as clamp_output leaves
        if budget >= 1024:
            body["thinking"] = {**th, "type": "enabled", "budget_tokens": budget}
        else:
            del body["thinking"]

    levels = supports.get("reasoning_effort")
    levels = [x for x in levels if isinstance(x, str)] if isinstance(levels, list) else []
    fitted = _fit_effort(body.get("output_config"), levels)
    if fitted is not body.get("output_config"):
        if fitted:
            body["output_config"] = fitted
        else:
            del body["output_config"]
    removed = False
    msgs = []
    for m in body["messages"]:
        fitted = _fit_effort(m.get("output_config"), levels)
        if fitted is m.get("output_config"):
            msgs.append(m)
        elif fitted or m.get("content"):  # still says something: keep it, with the fitted setting or without one
            msgs.append({**{k: v for k, v in m.items() if k != "output_config"},
                         **({"output_config": fitted} if fitted else {})})
        else:
            removed = True
    body["messages"] = msgs
    return removed


def context_management_for(body: dict[str, Any], mode: str) -> Optional[dict[str, Any]]:
    """The `context_management` block to send on a native request, or None to leave the body as it is.
    Edits the client already asked for are kept unchanged; the proxy only adds an edit type the client did not
    send (clear_thinking first, as the API requires)."""
    own = body.get("context_management")
    if mode == "off" or (own is not None and not (isinstance(own, dict) and isinstance(own.get("edits"), list))):
        return None
    edits: list[Any] = list(own["edits"]) if own else []
    types = {e.get("type") for e in edits if isinstance(e, dict) and isinstance(e.get("type"), str)}
    added = False
    th = body.get("thinking")
    if mode in ("thinking", "both") and isinstance(th, dict) and th.get("type") in ("enabled", "adaptive") \
            and "clear_thinking_20251015" not in types:
        edits.insert(0, {"type": "clear_thinking_20251015", "keep": {"type": "thinking_turns", "value": 1}})
        added = True
    n_tool_uses = sum(1 for m in _dicts(body.get("messages")) for b in _dicts(m.get("content"))
                      if b.get("type") == "tool_use")
    # Copilot evaluates the trigger on the full history every request, so a fixed `keep` clears one more tool
    # result on every tool turn and the whole cached prefix is rewritten each time (measured). Clearing in steps
    # of CLEAR_STEP tool uses keeps the cleared set, and so the cached prefix, unchanged between steps.
    n_clear = (max(0, n_tool_uses - CLEAR_KEEP) // CLEAR_STEP) * CLEAR_STEP
    if n_clear == 0 and n_tool_uses > CLEAR_KEEP:
        # fewer tool uses than one step: a few huge results can still pass the trigger, so clear per turn
        n_clear = n_tool_uses - CLEAR_KEEP
    if mode in ("tools", "both") and n_clear and "clear_tool_uses_20250919" not in types:
        edits.append({"type": "clear_tool_uses_20250919",
                      "trigger": {"type": "input_tokens", "value": CLEAR_TRIGGER},
                      "keep": {"type": "tool_uses", "value": n_tool_uses - n_clear}})
        added = True
    return {**(own or {}), "edits": edits} if added else None


def context_edit_refused(status: int, raw: bytes) -> bool:
    low = raw[:2000].lower()
    return status == 400 and any(s in low for s in (b"context_management", b"context management",
                                                    b"context-management", b"clear_tool_uses", b"clear_thinking"))


def cleared_input_tokens(ev: Any) -> Optional[int]:
    """Sum of `cleared_input_tokens` in a response or stream event's context_management.applied_edits."""
    if not isinstance(ev, dict):
        return None
    for holder in (ev, ev.get("delta"), ev.get("message")):
        edits = _dict_get(holder, "context_management").get("applied_edits")
        if isinstance(edits, list):
            return sum(e["cleared_input_tokens"] for e in _dicts(edits)
                       if isinstance(e.get("cleared_input_tokens"), int))
    return None


# ---------------------------------------------------------------------------
# Usage log
# ---------------------------------------------------------------------------


class UsageLog:
    """Per-request token accounting: optional JSONL file plus running totals for GET /stats."""

    FIELDS = ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens")

    def __init__(self, path: Optional[str]):
        self.path = Path(path).expanduser() if path else None
        self._lock = threading.Lock()
        self.started = time.time()
        self.totals: dict[str, Any] = {"requests": 0, "errors": 0, **{f: 0 for f in self.FIELDS},
                                       "cleared_input_tokens": 0, "by_route": {}, "by_model": {}}

    def record(self, **row: Any) -> None:
        raw = row.get("usage")
        raw = raw if isinstance(raw, dict) else {}
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
            if isinstance(row.get("cleared_input_tokens"), int):
                t["cleared_input_tokens"] += max(0, row["cleared_input_tokens"])
            buckets = [t] + [t[key].setdefault(name, {"requests": 0, **{f: 0 for f in self.FIELDS}})
                             for key, name in (("by_route", row.get("route")), ("by_model", row.get("model"))) if name]
            for bucket in buckets[1:]:
                bucket["requests"] += 1
            for bucket in buckets:
                for f in self.FIELDS:
                    bucket[f] += usage[f]
            if self.path:
                try:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    with self.path.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                except (OSError, UnicodeEncodeError) as exc:
                    LOG.warning("usage log write failed: %s", exc)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            t = json.loads(json.dumps(self.totals))
        prompt = t["input_tokens"] + t["cache_read_input_tokens"] + t["cache_creation_input_tokens"]
        t["prompt_tokens_total"] = prompt
        t["cache_hit_ratio"] = round(t["cache_read_input_tokens"] / prompt, 4) if prompt else 0.0
        t["uptime_s"] = int(time.time() - self.started)
        return t


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

ERROR_TYPES = {400: "invalid_request_error", 401: "authentication_error", 403: "permission_error",
               404: "not_found_error", 413: "request_too_large", 429: "rate_limit_error", 529: "overloaded_error"}


def anthropic_error(status: int, message: str) -> dict[str, Any]:
    return {"type": "error", "error": {"type": ERROR_TYPES.get(status, "api_error"), "message": message}}


class Reject(Exception):
    """Ends the current request with this status and message. Raised where a check fails, answered in one place
    (Handler._handle). close=True also ends the connection: the request body was not read."""

    def __init__(self, status: int, message: str, headers: Optional[dict[str, str]] = None, close: bool = False):
        super().__init__(message)
        self.status, self.message, self.headers, self.close = status, message, headers, close


def check_messages_request(payload: dict[str, Any]) -> None:
    """The fields the proxy itself reads must have the right type; the rest is for Copilot to judge."""
    msgs = payload.get("messages")
    if not isinstance(msgs, list):
        raise Reject(400, "messages: must be a list")
    if not all(isinstance(m, dict) for m in msgs):
        raise Reject(400, "messages: every item must be an object")
    mt = payload.get("max_tokens")
    if isinstance(mt, float) and mt.is_integer():
        payload["max_tokens"] = mt = int(mt)
    if mt is not None and (isinstance(mt, bool) or not isinstance(mt, int) or mt < 1):
        raise Reject(400, "max_tokens: must be a positive integer")


class _ProxyConfig:
    """Settings shared by the TCP and the Unix-socket server."""

    daemon_threads = True
    request_queue_size = 64  # Claude Code opens several connections at once

    def _setup(self, tm: TokenManager, catalog: ModelCatalog, guard: RequestGuard, show_token: bool,
               mode: str, count_mode: str, usage: Optional[UsageLog]) -> None:
        self.tm = tm
        self.catalog = catalog
        self.guard = guard
        self.show_token = show_token
        self.mode = mode              # auto | native | translate
        self.count_mode = count_mode  # auto | native | estimate
        self.usage = usage or UsageLog(None)
        self.cors_origins: set[str] = set()
        self.allowed_hosts = {"127.0.0.1", "localhost", "[::1]"}
        self.check_host = True
        self.auth_token: Optional[str] = None   # required client key (Authorization: Bearer / x-api-key)
        self.context_edit = "off"               # off | tools | thinking | both
        self.no_context_edit: set[str] = set()  # models that refused context_management
        self.is_unix = False

    def use_native(self, model: str) -> bool:
        """Whether a /v1/messages request for this model goes to Copilot's own /v1/messages."""
        return self.mode == "native" or (self.mode == "auto" and self.catalog.supports_endpoint(model, "/v1/messages"))

    def count_native(self, model: str) -> bool:
        return self.count_mode == "native" or (
            self.count_mode == "auto" and self.catalog.supports_endpoint(model, "/v1/messages"))


class ProxyServer(_ProxyConfig, ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(self, addr: tuple[str, int], tm: TokenManager, catalog: ModelCatalog,
                 guard: RequestGuard, show_token: bool, mode: str = "auto", count_mode: str = "auto",
                 usage: Optional[UsageLog] = None):
        self._setup(tm, catalog, guard, show_token, mode, count_mode, usage)
        self.allowed_hosts |= {h for h in [host_name(str(addr[0]))] if h}
        # binding to all interfaces is an explicit choice to accept any Host
        self.check_host = addr[0] not in ("0.0.0.0", "::", "")
        if ":" in addr[0]:
            self.address_family = socket.AF_INET6
        super().__init__(addr, Handler)


# Windows has no socketserver.UnixStreamServer; --unix-socket is refused there (cmd_start)
_UnixBase = getattr(socketserver, "UnixStreamServer", socketserver.TCPServer)


class UnixProxyServer(_ProxyConfig, socketserver.ThreadingMixIn, _UnixBase):  # type: ignore[misc, valid-type]
    """Listens on a Unix socket only (Claude Code: ANTHROPIC_UNIX_SOCKET). The socket file is mode 0600, so only
    this user can connect, and there is no TCP port for other local processes or sandboxed commands."""

    def __init__(self, path: str, tm: TokenManager, catalog: ModelCatalog, guard: RequestGuard,
                 show_token: bool, mode: str = "auto", count_mode: str = "auto",
                 usage: Optional[UsageLog] = None):
        self._setup(tm, catalog, guard, show_token, mode, count_mode, usage)
        self.is_unix = True
        self.check_host = False  # no TCP listener, so no DNS rebinding; Origin is still checked
        p = Path(path).expanduser().absolute()
        self.socket_path = p  # before bind: a failed bind calls server_close()
        self._owns_socket = False
        private_dir(p.parent)
        if p.exists() or p.is_symlink():
            if not stat.S_ISSOCK(p.lstat().st_mode):
                raise SystemExit(f"{p} exists and is not a socket; refusing to replace it")
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            probe.settimeout(2)
            try:
                probe.connect(str(p))
                raise SystemExit(f"Another proxy is already listening on {p}")
            except (ConnectionRefusedError, FileNotFoundError):
                p.unlink(missing_ok=True)  # nobody listens: stale socket from an earlier run
            except OSError as exc:
                raise SystemExit(f"Cannot check the existing socket {p}: {exc}")
            finally:
                probe.close()
        old = os.umask(0o177)
        try:
            super().__init__(str(p), Handler)
        finally:
            os.umask(old)
        os.chmod(str(p), 0o600)
        self._owns_socket = True

    def server_close(self) -> None:
        super().server_close()
        if getattr(self, "_owns_socket", False):  # never remove a socket this process did not create
            try:
                self.socket_path.unlink()
            except OSError:
                pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: Any  # ProxyServer | UnixProxyServer
    _streaming = False  # the SSE answer has started: a failure can only be sent as an event
    _answered = False   # the whole answer is out: nothing more may be written for this request
    _anthropic = True   # shape of an error body: Anthropic, or OpenAI on the OpenAI routes

    def setup(self) -> None:
        super().setup()
        if self.connection.family in (socket.AF_INET, socket.AF_INET6):
            # send every SSE event at once instead of holding it until the previous packet is acknowledged
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def address_string(self) -> str:
        ca = self.client_address
        return ca[0] if isinstance(ca, tuple) and ca else "unix"

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.info("%s %s", self.address_string(), fmt % args)

    # -- one request -------------------------------------------------------

    def do_GET(self) -> None:
        self._handle(self._get)

    def do_POST(self) -> None:
        self._handle(self._post)

    def do_OPTIONS(self) -> None:
        self._handle(self._preflight)

    def do_HEAD(self) -> None:
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _handle(self, route: Callable[[], None]) -> None:
        """Access checks, then the route. Whatever fails, the client gets an answer (an HTTP error, or an SSE
        error event once a stream has started) and every connection to Copilot is closed or back in the pool."""
        self._streaming, self._answered, self._anthropic = False, False, True
        self._upstreams: list[UpstreamResponse] = []
        try:
            self._check_access()
            route()
        except Reject as exc:
            self.close_connection = self.close_connection or exc.close
            self._fail(exc.status, exc.message, exc.headers)
        except ConnectionError as exc:  # the client or Copilot went away
            LOG.info("Connection error: %s", exc)
            self.close_connection = True
            self._fail(502, f"Upstream connection error: {exc}")
        except (OSError, http.client.HTTPException, UpstreamError) as exc:
            LOG.warning("%s %s: %s: %s", self.command, self._path(), type(exc).__name__, exc)
            self._fail(502, f"Upstream error: {exc}")
        except Exception as exc:
            LOG.exception("%s %s failed", self.command, self._path())
            self._fail(500, f"Proxy error: {type(exc).__name__}: {exc}")
        finally:
            for up in self._upstreams:
                up.close()

    def _path(self) -> str:
        return urllib.parse.urlsplit(self.path).path.rstrip("/") or "/"  # Claude Code adds ?beta=true

    def _check_access(self) -> None:
        """Block DNS rebinding (unexpected Host), cross-site browser requests (Origin not allowed) and, with
        --auth-token, requests without the key."""
        srv = self.server
        host = self.headers.get("Host") or ""
        origin = self.headers.get("Origin")
        status, reason = 403, None
        if srv.check_host and host_name(host) not in srv.allowed_hosts:
            reason = f"Host '{host}' not allowed (use --allow-host)"
        elif origin and not ("*" in srv.cors_origins or origin in srv.cors_origins):
            reason = f"Origin '{origin}' not allowed (use --cors-origin)"
        elif srv.auth_token and self._path() not in ("/", "/health") and self.command != "OPTIONS" \
                and not self._key_ok():
            status, reason = 401, "Missing or wrong proxy key (Authorization: Bearer <key> or x-api-key)"
        if reason:
            raise Reject(status, reason, {"Connection": "close"}, close=True)

    def _key_ok(self) -> bool:
        want = self.server.auth_token.encode()
        auth = self.headers.get("Authorization") or ""
        keys = [self.headers.get("x-api-key") or "", auth[7:].strip() if auth[:7].lower() == "bearer " else ""]
        ok = False
        for k in keys:  # no short-circuit: compare both in constant time
            ok |= bool(k) and hmac.compare_digest(k.encode(), want)
        return ok

    # -- answers -----------------------------------------------------------

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

    def _send_raw(self, status: int, body: bytes, ctype: str = JSON_TYPE,
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
        self._answered = True

    def _send_json(self, status: int, body: Any, extra: Optional[dict[str, str]] = None) -> None:
        self._send_raw(status, _dumps(body), extra=extra)

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
            self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))

    def _end_chunks(self) -> None:
        self.wfile.write(b"0\r\n\r\n")
        self._answered = True

    def _fail(self, status: int, message: str, headers: Optional[dict[str, str]] = None) -> None:
        """Before the answer started: an HTTP error. After an SSE stream started: an error event."""
        if self._answered:  # a failure after the answer was complete: only make sure nothing follows it
            self.close_connection = True
            return
        if self._anthropic:
            body = anthropic_error(status, message)
        else:
            body = {"error": {"message": message, "type": ERROR_TYPES.get(status, "api_error")}}
        try:
            if self._streaming:
                self._chunk(sse("error", body) if self._anthropic else b"data: " + _dumps(body) + b"\n\n")
                self._end_chunks()
            else:
                self._send_json(status, body, headers)
        except OSError:
            self.close_connection = True  # the client is gone

    def _relay(self, up: UpstreamResponse, on_data: Callable[[dict[str, Any]], None],
               marks: tuple[bytes, ...] = (b'"usage"',)) -> None:
        """Forward Copilot's SSE stream unchanged, in the pieces it arrives in. `on_data` gets the JSON of every
        `data:` line that contains one of `marks` (the lines with the token counts)."""
        def scan(lines: list[bytes]) -> None:
            for line in lines:
                if line.startswith(b"data:") and any(m in line for m in marks):
                    on_data(_json_object(line[5:]))

        self._begin_sse()
        tail = b""
        for block in up.blocks():
            self._chunk(block)
            *lines, tail = (tail + block).split(b"\n")
            scan(lines)
        scan([tail])
        self._end_chunks()

    # -- request -----------------------------------------------------------

    def _read_body(self) -> bytes:
        """The request body (Content-Length or chunked), at most MAX_BODY bytes. A body that cannot be framed
        also ends the connection, because the next request could not be found in it."""
        def bad(status: int, message: str) -> Reject:
            return Reject(status, message, close=True)

        codings = [t.strip().lower() for h in (self.headers.get_all("Transfer-Encoding") or []) for t in h.split(",")]
        if codings and codings[-1] != "chunked":
            raise bad(400, "Unsupported Transfer-Encoding")
        if not codings:
            lengths = {v.strip() for v in (self.headers.get_all("Content-Length") or [])}
            if not lengths:
                return b""
            value = lengths.pop()
            if lengths or not (value.isascii() and value.isdigit()):  # two different values, or not a number
                raise bad(400, "Bad Content-Length")
            if int(value) > MAX_BODY:
                raise bad(413, f"Request body over {MAX_BODY} bytes")
            raw = self.rfile.read(int(value))
            if len(raw) != int(value):
                raise bad(400, "Truncated request body")
            return raw
        parts, total = [], 0
        while True:
            line = self.rfile.readline(1024)
            field = line.split(b";")[0].strip()  # the chunk size in hex; chunk extensions are ignored
            if not line.endswith(b"\n") or not field or not all(c in b"0123456789abcdefABCDEF" for c in field):
                raise bad(400, "Malformed chunked body")
            size = int(field, 16)
            if size == 0:
                break
            total += size
            if total > MAX_BODY:
                raise bad(413, f"Request body over {MAX_BODY} bytes")
            data = self.rfile.read(size)
            if len(data) != size or self.rfile.readline(8) not in (b"\r\n", b"\n"):
                raise bad(400, "Truncated chunked body")
            parts.append(data)
        for _ in range(64):  # trailers, bounded
            if self.rfile.readline(1024) in (b"\r\n", b"\n", b""):
                break
        else:
            self.close_connection = True  # unread trailers: do not reuse the connection
        return b"".join(parts)

    def _read_json(self) -> dict[str, Any]:
        raw = self._read_body()
        try:
            body = json.loads(raw) if raw else {}
        except (ValueError, RecursionError) as exc:
            raise Reject(400, f"Invalid JSON body: {exc}") from None
        if not isinstance(body, dict):
            raise Reject(400, "JSON body must be an object")
        return body

    def _guard(self, summary: str) -> None:
        ok, wait = self.server.guard.check(summary)
        if not ok and wait is None:
            raise Reject(403, "Request rejected by manual approval.")
        if not ok:
            raise Reject(429, f"Local rate limit. Retry after {wait:.1f}s.",
                         {"Retry-After": str(max(1, math.ceil(wait)))})

    # -- Copilot -----------------------------------------------------------

    def _copilot(self, path: str, payload: dict[str, Any], agent: bool = False, vision: bool = False,
                 extra_headers: Optional[dict[str, str]] = None) -> UpstreamResponse:
        """POST to Copilot. Retries once on 401 (token refresh) and up to RETRIES times on 429/5xx, before any
        byte reaches the client, so Claude Code does not re-send the whole prompt after a long backoff."""
        body = _dumps(payload)
        refreshed = force = False
        retries = 0
        while True:
            token, base = self.server.tm.get_token(force_refresh=force)
            force = False
            headers = {**self.server.tm.copilot_headers(token, agent, vision), **(extra_headers or {})}
            up = UPSTREAM.post(base, path, headers, body)
            self._upstreams.append(up)
            wait = None
            if up.status == 401 and not refreshed:
                refreshed = force = True
                wait = 0.0
                LOG.warning("Copilot 401; refreshing token and retrying")
            elif up.status in RETRY_STATUSES and retries < RETRIES:
                wait = _retry_after(up.header("Retry-After"), retries)
                if wait is not None:
                    retries += 1
                    LOG.warning("Copilot %d; retry %d in %.1fs", up.status, retries, wait)
            if wait is None:
                return up
            up.body()  # read to the end, so the connection can be used again
            time.sleep(wait)

    def _native_headers(self, without_per_turn: bool = False) -> dict[str, str]:
        """The client's Anthropic version and betas for Copilot's Anthropic endpoints."""
        h = {"anthropic-version": self.headers.get("anthropic-version") or "2023-06-01"}
        betas = [b.strip() for value in self.headers.get_all("anthropic-beta") or [] for b in value.split(",")]
        if without_per_turn:  # fit_to_model removed the per-turn message: its beta goes with it
            betas = [b for b in betas if not b.startswith("per-turn-control")]
        betas = [b for b in betas if b]
        if betas and os.environ.get("COPILOT_PROXY_DROP_BETA") != "1":
            h["anthropic-beta"] = ",".join(betas)
        return h

    @staticmethod
    def _passthrough_headers(up: UpstreamResponse) -> dict[str, str]:
        """Upstream headers worth keeping on errors: Claude Code backs off on retry-after."""
        return {k: up.header(k) for k in ("retry-after", "x-request-id", "request-id")}

    def _record(self, log: dict[str, Any], started: float, status: int, usage: Any = None) -> None:
        self.server.usage.record(**log, status=status, usage=usage, ms=int((time.monotonic() - started) * 1000))

    # -- routes ------------------------------------------------------------

    def _preflight(self) -> None:
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _get(self) -> None:
        srv, path = self.server, self._path()
        if path in ("/", "/health"):
            self._send_json(200, {"status": "ok", "service": "copilot-proxy-stdlib",
                                  "api_base_url": srv.tm.api_base_url})
        elif path in ("/models", "/v1/models"):
            self._send_json(200, srv.catalog.get_models_payload())
        elif path == "/usage":
            status, data = srv.tm.fetch_usage()
            self._send_json(status or 502, data)
        elif path == "/stats":
            self._send_json(200, srv.usage.snapshot())
        elif path == "/token":
            if not srv.show_token:
                raise Reject(403, "Disabled. Start with --show-token to enable.")
            token, base = srv.tm.get_token()
            self._send_json(200, {"token": token, "api_base_url": base})
        else:
            raise Reject(404, f"Not found: {path}")

    def _post(self) -> None:
        path = self._path()
        route = path[3:] if path.startswith("/v1/") else path
        if route == "/messages":
            self._messages()
        elif route == "/messages/count_tokens":
            self._count_tokens()
        elif route in ("/chat/completions", "/responses"):
            self._openai_route(route)
        elif route == "/embeddings":
            self._embeddings()
        else:
            self._read_body()  # so the connection can be used again
            raise Reject(404, f"Not found: {path}")

    def _messages(self) -> None:
        payload = self._read_json()
        check_messages_request(payload)
        requested = str(payload.get("model") or "")
        model = self.server.catalog.resolve_model(requested)
        stream = bool(payload.get("stream"))
        self._guard(f"POST /v1/messages model={model} stream={stream}")
        if not (self.server.use_native(model) and self._messages_native(payload, requested, model, stream)):
            self._messages_translated(payload, requested, model, stream)

    def _messages_native(self, payload: dict[str, Any], requested: str, model: str, stream: bool) -> bool:
        """Forward the Anthropic request unchanged (model id mapped, max_tokens and effort fitted to the model)
        to Copilot's own /v1/messages. Keeps cache_control breakpoints, thinking blocks and signatures intact.
        Returns False (nothing sent to the client) when Copilot refuses the route itself in auto mode, so the
        caller can fall back to the translated route."""
        started = time.monotonic()
        srv = self.server
        body = {**payload, "model": model}
        clamp_output(body, srv.catalog.max_output_tokens(model))
        agent, vision = anthropic_flags(body)
        client_headers = self._native_headers(without_per_turn=fit_to_model(body, srv.catalog.supports(model)))
        LOG.info("messages(native): %s -> %s stream=%s initiator=%s vision=%s msgs=%d tools=%d", requested, model,
                 stream, "agent" if agent else "user", vision, len(body["messages"]), len(body.get("tools") or []))
        log = dict(route="native", model=model, stream=stream, initiator="agent" if agent else "user")
        headers = dict(client_headers)
        own_cm = body.get("context_management")
        if own_cm is not None:
            LOG.debug("client context_management: %s", json.dumps(own_cm)[:500])
        cm = None if model in srv.no_context_edit else context_management_for(body, srv.context_edit)
        if cm:
            body["context_management"] = cm
            betas = [b for b in headers.get("anthropic-beta", "").split(",") if b]
            if CONTEXT_MANAGEMENT_BETA not in betas:
                headers["anthropic-beta"] = ",".join(betas + [CONTEXT_MANAGEMENT_BETA])
            log["context_edit"] = srv.context_edit
        up = self._copilot("/v1/messages", body, agent, vision, headers)
        if cm and up.status == 400 and context_edit_refused(up.status, up.body()):
            # send the client's own request once more, as it would have gone without the proxy's edit; stop
            # adding the edit for this model only when Copilot says the strategy is unsupported (the same
            # recovery Claude Code uses)
            refusal = up.body()
            LOG.warning("Copilot rejected the added context_management for %s (%s); resending without it",
                        model, refusal[:200])
            if any(s in refusal.lower() for s in (b"does not support", b"not supported", b"unsupported")):
                srv.no_context_edit.add(model)
            if own_cm is None:
                del body["context_management"]
            else:
                body["context_management"] = own_cm
            log["context_edit"] = "rejected"
            up = self._copilot("/v1/messages", body, agent, vision, client_headers)

        if up.status != 200 or not stream:
            raw = up.body()
            if up.status != 200:
                LOG.warning("Copilot native error %d: %s", up.status, raw[:500])
                if srv.mode == "auto" and _route_unsupported(up.status, raw):
                    # only when the endpoint/model itself is refused; a real 400 (context too long, bad
                    # request) goes back to Claude Code instead of re-sending the whole prompt elsewhere
                    LOG.warning("Native route not available (%d); falling back to the translated route", up.status)
                    return False
            answer = _json_object(raw)
            if cleared_input_tokens(answer):
                log["cleared_input_tokens"] = cleared_input_tokens(answer)
            self._record(log, started, up.status, _dict_get(answer, "usage"))
            self._send_raw(up.status, raw, up.header("Content-Type", JSON_TYPE),
                           self._passthrough_headers(up) if up.status != 200 else None)
            return True

        usage: dict[str, int] = {}

        def scan(ev: dict[str, Any]) -> None:
            if cleared_input_tokens(ev):
                log["cleared_input_tokens"] = cleared_input_tokens(ev)
            holder = _dict_get(ev, "message") if ev.get("type") == "message_start" else ev
            usage.update({k: v for k, v in _dict_get(holder, "usage").items() if isinstance(v, int) and v})

        self._relay(up, scan, (b'"usage"', b'"context_management"'))
        self._record(log, started, 200, usage)
        return True

    def _messages_translated(self, payload: dict[str, Any], requested: str, model: str, stream: bool) -> None:
        """Send the request as /chat/completions and translate the answer back, streamed or whole."""
        started = time.monotonic()
        try:
            oai, agent, vision = anthropic_to_openai_request(
                payload, model, self.server.catalog.max_output_tokens(model))
        except (ValueError, TypeError, AttributeError) as exc:
            raise Reject(400, f"Invalid request: {exc}") from None
        LOG.info("messages: %s -> %s stream=%s initiator=%s vision=%s msgs=%d tools=%d", requested, model,
                 stream, "agent" if agent else "user", vision, len(oai["messages"]), len(oai.get("tools", [])))
        log = dict(route="translate", model=model, stream=stream, initiator="agent" if agent else "user")
        up = self._copilot("/chat/completions", oai, agent, vision)
        if up.status != 200:
            err = up.body().decode("utf-8", errors="replace")
            LOG.warning("Copilot error %d: %s", up.status, err[:500])
            self._record(log, started, up.status)
            raise Reject(up.status, err, self._passthrough_headers(up))
        if not stream:
            try:
                out = openai_to_anthropic_response(json.loads(up.body()), requested or model)
            except (ValueError, TypeError, AttributeError) as exc:
                raise UpstreamError(f"Copilot's answer could not be read: {exc}") from None
            self._record(log, started, 200, out["usage"])
            self._send_json(200, out)
            return

        tr = StreamTranslator(requested or model)
        self._begin_sse()
        done = False
        for line in up.lines():  # read to the end even after [DONE], so the connection can be used again
            if done or not line.startswith(b"data:"):
                continue
            data = line[5:].strip()
            if data == b"[DONE]":
                done = True
                continue
            chunk = _json_object(data)
            if chunk.get("error"):
                error = chunk["error"]
                raise UpstreamError("Copilot error in the stream: "
                                    + str(error.get("message") if isinstance(error, dict) else error))
            self._chunk(b"".join(tr.feed(chunk)))
        if not (done or tr.finish_seen):
            raise UpstreamError("Copilot closed the stream before the answer was complete")
        self._chunk(b"".join(tr.finalize()))
        self._end_chunks()
        self._record(log, started, 200, dict(tr.usage))

    def _count_tokens(self) -> None:
        payload = self._read_json()
        check_messages_request(payload)
        model = self.server.catalog.resolve_model(str(payload.get("model") or ""))
        if self.server.count_native(model):
            body = {k: payload[k] for k in ("system", "messages", "tools", "tool_choice", "thinking") if k in payload}
            body["model"] = model
            try:
                up = self._copilot("/v1/messages/count_tokens", body, False, anthropic_flags(body)[1],
                                   self._native_headers())
                count = _json_object(up.body()).get("input_tokens")
                if up.status == 200 and isinstance(count, int):
                    self._send_json(200, {"input_tokens": count})
                    return
                LOG.warning("native count_tokens failed (%s); using the estimate", up.status)
            except (OSError, http.client.HTTPException, UpstreamError) as exc:
                LOG.warning("native count_tokens error: %s; using the estimate", exc)
        self._send_json(200, {"input_tokens": estimate_tokens(payload)})

    def _openai_route(self, upstream: str) -> None:
        """OpenAI-format pass-through (/chat/completions, /responses), streaming kept byte for byte."""
        self._anthropic = False
        payload = self._read_json()
        cat = self.server.catalog
        if payload.get("model"):
            name = str(payload["model"])
            if cat.only and not cat.family_id(name):
                raise Reject(400, f"model {name}: only {', '.join(cat.only.values())} are enabled "
                                  "(start the proxy with --any-model for the full Copilot catalog)")
            payload["model"] = cat.resolve_model(name)
        stream = bool(payload.get("stream"))
        agent, vision = openai_flags(payload)
        self._guard(f"POST {upstream} model={payload.get('model')} stream={stream}")
        started = time.monotonic()
        log = dict(route=upstream.strip("/"), model=payload.get("model"), stream=stream,
                   initiator="agent" if agent else "user")
        up = self._copilot(upstream, payload, agent, vision)
        if up.status != 200 or not stream:
            raw = up.body()
            self._record(log, started, up.status, openai_usage(_dict_get(_json_object(raw), "usage")))
            self._send_raw(up.status, raw, up.header("Content-Type", JSON_TYPE),
                           self._passthrough_headers(up) if up.status != 200 else None)
            return
        usage: dict[str, int] = {}

        def scan(ev: dict[str, Any]) -> None:
            u = _dict_get(ev, "usage") or _dict_get(_dict_get(ev, "response"), "usage")
            if u:  # chat: the last chunk; responses: response.completed
                usage.clear()
                usage.update(openai_usage(u))

        self._relay(up, scan)
        self._record(log, started, 200, usage)

    def _embeddings(self) -> None:
        self._anthropic = False
        payload = self._read_json()
        self._guard(f"POST /embeddings model={payload.get('model')}")
        up = self._copilot("/embeddings", payload)
        self._send_raw(up.status, up.body(), up.header("Content-Type", JSON_TYPE))


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
                    subagent: Optional[str] = None, unix_socket: Optional[str] = None,
                    auth_token: Optional[str] = None) -> dict[str, str]:
    """Claude Code settings for this proxy. Variable names checked against the Claude Code 2.1.330 binary."""
    sonnet = catalog.resolve_model(primary) if primary else (catalog.newest("sonnet") or PREFERRED_MODELS["sonnet"])
    haiku = catalog.resolve_model(small) if small else (catalog.newest("haiku") or sonnet)
    opus = catalog.newest("opus") or sonnet
    subagent = catalog.resolve_model(subagent) if subagent else None
    env = {
        "ANTHROPIC_BASE_URL": base,
        **({"ANTHROPIC_UNIX_SOCKET": unix_socket} if unix_socket else {}),
        "ANTHROPIC_AUTH_TOKEN": auth_token or "dummy",
        "ANTHROPIC_MODEL": sonnet,
        "ANTHROPIC_DEFAULT_SONNET_MODEL": sonnet,
        "ANTHROPIC_DEFAULT_OPUS_MODEL": opus,
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": haiku,
        # Each subagent starts with a fresh user prompt, which Copilot bills at the subagent model's rate
        # (legacy plans: Sonnet 4.6 = 9, Haiku 4.5 = 0.33 premium requests). Override with --subagent-model.
        "CLAUDE_CODE_SUBAGENT_MODEL": subagent or haiku,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        # Auto mode: Copilot rejects the server-side review (400 "safeguards: Extra inputs are not permitted",
        # measured), so Claude Code would fall back to its own classifier after a failed request and a notice.
        # 0 = use its own classifier from the start. Each checked action is then one more model request.
        "CLAUDE_CODE_AUTO_MODE_SERVER": "0",
    }
    window = catalog.max_prompt_tokens(sonnet)
    if window:
        # compact against Copilot's real prompt limit, not the model's native window
        env["CLAUDE_CODE_AUTO_COMPACT_WINDOW"] = str(window)
    return env


def secret_paths(args: argparse.Namespace) -> list[str]:
    """Files that hold the GitHub login. Claude Code's sandboxed commands can read the whole disk by default."""
    paths = [args.token_file] + ([] if getattr(args, "no_copilot_config", False) else [args.copilot_config_dir])
    out: list[str] = []
    for p in paths:
        for q in sorted(_both(p)):
            if q not in out:
                out.append(q)
    return out


def sandbox_settings(args: argparse.Namespace) -> dict[str, Any]:
    """Claude Code settings that keep the GitHub login and the proxy away from what the model runs:
    - sandbox.filesystem.denyRead: sandboxed Bash commands cannot read the login files (the default lets them
      read the whole disk);
    - permissions.deny Read(//...): the built-in Read tool runs outside the sandbox, so it needs its own rule;
    - CLAUDE_CODE_SUBPROCESS_ENV_SCRUB (added to env): Bash, hooks and MCP servers do not inherit the proxy key;
    - no allowedDomains / allowUnixSockets entry for the proxy, so a sandboxed command cannot spend the Copilot
      account. Claude Code's own model calls run outside the sandbox and are not affected."""
    paths = secret_paths(args)
    deny = []
    for p in paths:
        deny += [f"Read(/{p})", f"Read(/{p}/**)"]  # '//' = absolute path in permission rules
    return {"permissions": {"deny": deny},
            "sandbox": {"enabled": True, "failIfUnavailable": True, "allowUnsandboxedCommands": False,
                        "filesystem": {"denyRead": paths}}}


def real_home() -> str:
    """The account's home from the password database: $HOME can be overridden to point anywhere."""
    try:
        import pwd
        return os.path.realpath(pwd.getpwuid(os.getuid()).pw_dir)
    except (ImportError, KeyError):
        return os.path.realpath(os.path.expanduser("~"))


def _both(path: str) -> set[str]:
    """A path as given and with symlinks resolved (Seatbelt and permission rules match either literally)."""
    a = os.path.abspath(os.path.expanduser(path))
    return {a, os.path.realpath(a)}


def _sb_quote(path: str) -> str:
    return '"' + path.replace("\\", "\\\\").replace('"', '\\"') + '"'


def self_sandbox_profile(args: argparse.Namespace) -> str:
    """macOS Seatbelt profile for the proxy itself: no access to the home directory except what it needs (its
    script, its GitHub login, its usage log, its socket), so a bug in the proxy cannot read ~/.ssh and the like."""
    home = real_home()
    script = os.path.realpath(os.path.abspath(sys.argv[0]))
    python = os.path.realpath(sys.executable)
    prefixes = {os.path.realpath(p) for p in (sys.prefix, sys.base_prefix, sys.exec_prefix)}
    # only the script file itself: its directory may be the home directory (python runs with -I, so the script
    # directory is not on sys.path)
    read = {script} | prefixes
    for p in secret_paths(args):
        read |= _both(p)
    rw = _both(os.path.dirname(os.path.abspath(os.path.expanduser(args.token_file))))
    if getattr(args, "usage_log", None):
        rw |= _both(args.usage_log)
    if getattr(args, "unix_socket", None):
        rw |= _both(os.path.dirname(os.path.abspath(os.path.expanduser(args.unix_socket))))
    for d in read | rw:
        if d == home or home.startswith(d.rstrip("/") + "/"):
            raise SystemExit(f"--sandbox-self: {d} would open the whole home directory; put the token file, "
                             "socket and usage log in a subdirectory (the defaults do)")
    # stat() on the directories between the home directory and each allowed path (not their contents)
    ancestors = {home}
    for path in read | rw:
        cur = os.path.dirname(path)
        while cur.startswith(home + "/"):
            ancestors.add(cur)
            cur = os.path.dirname(cur)

    def sub(paths: set[str]) -> str:
        return " ".join(f"(subpath {_sb_quote(p)})" for p in sorted(paths))

    return "\n".join([
        "(version 1)",
        "(allow default)",
        f"(deny file-read* file-write* (subpath {_sb_quote(home)}))",
        f"(allow file-read* file-read-metadata {sub(read | rw)})",
        f"(allow file-write* {sub(rw)})",
        "(allow file-read-metadata " + " ".join(f"(literal {_sb_quote(p)})" for p in sorted(ancestors)) + ")",
        # the proxy starts no programs: only the interpreter (a framework build re-executes inside its prefix)
        "(deny process-exec*)",
        f"(allow process-exec* (literal {_sb_quote(python)}) {sub(prefixes)})",
    ])


def _home_unreadable() -> bool:
    try:
        os.listdir(real_home())
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def reexec_sandboxed(args: argparse.Namespace) -> None:
    if os.environ.get("COPILOT_PROXY_SANDBOXED") == "1":
        if not _home_unreadable():  # the variable alone proves nothing
            raise SystemExit("--sandbox-self: COPILOT_PROXY_SANDBOXED is set but the home directory is readable; "
                             "not running inside the sandbox")
        return
    if sys.platform != "darwin" or not Path("/usr/bin/sandbox-exec").exists():
        raise SystemExit("--sandbox-self needs macOS sandbox-exec. On Linux, run the proxy under bwrap; see README.")
    profile = self_sandbox_profile(args)
    # create the writable directories now: inside the sandbox their missing parents cannot be created
    private_dir(Path(args.token_file).expanduser().absolute().parent)
    if getattr(args, "unix_socket", None):
        private_dir(Path(args.unix_socket).expanduser().absolute().parent)
    if getattr(args, "usage_log", None):
        Path(args.usage_log).expanduser().absolute().parent.mkdir(parents=True, exist_ok=True)
    LOG.debug("sandbox profile:\n%s", profile)
    argv = [a for a in sys.argv[1:] if a != "--sandbox-self"] + ["--sandbox-self"]  # child re-checks (above)
    os.environ["COPILOT_PROXY_SANDBOXED"] = "1"
    os.execv("/usr/bin/sandbox-exec", ["sandbox-exec", "-p", profile, os.path.realpath(sys.executable), "-I",
                                        os.path.realpath(os.path.abspath(sys.argv[0])), *argv])


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


def print_banner(args: argparse.Namespace, tm: TokenManager, catalog: ModelCatalog, base: str,
                 sock: Optional[str], auth: Optional[str]) -> None:
    """What the proxy listens on and how it is set up; with -c, the Claude Code settings to use it."""
    print(f"\nCopilot proxy listening on {'unix socket ' + sock if sock else base}"
          + (" (inside macOS sandbox-exec)" if os.environ.get("COPILOT_PROXY_SANDBOXED") == "1" else ""))
    via = upstream_proxy("https", urllib.parse.urlsplit(tm.api_base_url).hostname or "")
    print(f"Upstream: {tm.api_base_url}" + (f" via HTTP proxy {via[0]}:{via[1]}" if via else ""))
    ids = catalog.chat_model_ids()
    native = [m for m in ids if catalog.supports_endpoint(m, "/v1/messages")]
    print(f"Messages route: {args.mode} (native /v1/messages models: {', '.join(native) or 'none'}); "
          f"count_tokens: {args.count_tokens}; context editing: {args.context_edit}"
          + (f" (above {CLEAR_TRIGGER} input tokens, clear old tool results in steps of {CLEAR_STEP}, "
             f"keep at least {CLEAR_KEEP})"
             if args.context_edit in ("tools", "both") else "")
          + (f"; usage log: {args.usage_log}" if args.usage_log else ""))
    print("Client key: " + ("required" if auth else "none (socket is mode 0600: only your user can connect)" if sock
                            else "none (any local process can use this proxy; see --auth-token)"))
    if catalog.only:
        print(f"Models: only {', '.join(catalog.only.values())} (every request maps to one of these by family; "
              "--any-model for the full catalog)")
        if catalog.missing():
            LOG.warning("This Copilot account does not list %s yet: requests for it will fail upstream",
                        ", ".join(catalog.missing()))
    else:
        print(f"Chat models ({len(ids)}): {', '.join(ids)}")
    if args.claude_code:
        env = claude_code_env(base, catalog, args.model, args.small_model, args.subagent_model, sock, auth)
        if args.sandbox:
            env["CLAUDE_CODE_SUBPROCESS_ENV_SCRUB"] = "1"
        print("\nRun Claude Code in another terminal:\n  " + " ".join(f"{k}={shlex.quote(v)}" for k, v in env.items())
              + " claude")
        # Measured with Claude Code 2.1.330: the socket and its connection settings must be in the shell
        # environment; from settings.json "env" they apply too late (connection refused / TLS on the socket)
        shell_only = ("ANTHROPIC_UNIX_SOCKET", "ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN") if sock else ()
        settings: dict[str, Any] = {"env": {k: v for k, v in env.items() if k not in shell_only}}
        if args.sandbox:
            settings.update(sandbox_settings(args))
        if sock:
            print("\nOr export these in the shell (they do not work from settings.json):\n  export "
                  + " ".join(f"{k}={shlex.quote(env[k])}" for k in shell_only))
        print(("\nand put this in" if sock else "\nOr put this in") + " .claude/settings.json"
              + (" (keep it out of git: it holds the proxy key)" if auth and not sock else "")
              + ":\n" + json.dumps(settings, indent=2))
    elif auth:
        print("\nClient key (send as Authorization: Bearer or x-api-key):", auth)
    print("\nCtrl+C to stop.\n", flush=True)


def cmd_start(args: argparse.Namespace) -> None:
    if args.context_edit not in CONTEXT_EDIT_MODES:  # an env default is not checked by argparse
        raise SystemExit(f"--context-edit / COPILOT_PROXY_CONTEXT_EDIT must be one of {', '.join(CONTEXT_EDIT_MODES)}")
    if args.unix_socket and not hasattr(socketserver, "UnixStreamServer"):
        raise SystemExit("--unix-socket is not available on this platform")
    if args.sandbox_self:
        reexec_sandboxed(args)  # does not return unless already inside the sandbox
    elif os.environ.get("COPILOT_PROXY_SANDBOXED") == "1" and not _home_unreadable():
        os.environ.pop("COPILOT_PROXY_SANDBOXED")  # a stray variable must not make the banner claim a sandbox
    auth = secrets.token_urlsafe(24) if args.auth_token == "auto" else (args.auth_token or None)
    tm = TokenManager(resolve_github_token(args), account_type=args.account_type, show_token=args.show_token)
    tm.start_background_refresh()
    catalog = ModelCatalog(tm, primary=args.model, small=args.small_model, any_model=args.any_model)
    catalog.refresh(0.0)  # a failure is logged; the list is fetched again when a request needs it
    guard = RequestGuard(args.rate_limit, args.wait, args.manual)
    kw = dict(mode=args.mode, count_mode=args.count_tokens, usage=UsageLog(args.usage_log))
    if args.unix_socket:
        server: Any = UnixProxyServer(args.unix_socket, tm, catalog, guard, args.show_token, **kw)
        sock: Optional[str] = str(server.socket_path)
        base = "http://localhost"
    else:
        try:
            server = ProxyServer((args.host, args.port), tm, catalog, guard, args.show_token, **kw)
        except OSError as exc:
            raise SystemExit(f"copilot_proxy: cannot listen on {args.host}:{args.port}: {exc}")
        sock = None
        shown = f"[{args.host}]" if ":" in args.host and not args.host.startswith("[") else args.host
        base = f"http://{shown}:{args.port}"
    server.cors_origins = set(args.cors_origin or [])
    server.allowed_hosts |= {h for h in (host_name(a) for a in (args.allow_host or [])) if h}
    server.auth_token = auth
    server.context_edit = args.context_edit
    if not server.check_host and not sock:
        LOG.warning("Bound to %s: any Host is accepted and anyone who can reach this port uses your Copilot account",
                    args.host)
    print_banner(args, tm, catalog, base, sock, auth)

    def on_term(signum: int, frame: Any) -> None:
        raise KeyboardInterrupt  # same clean shutdown as Ctrl+C (removes the Unix socket)
    signal.signal(signal.SIGTERM, on_term)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        tm.stop()
        server.server_close()
        UPSTREAM.clear()


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
    s.add_argument("--any-model", action="store_true", default=os.environ.get("COPILOT_PROXY_ANY_MODEL") == "1",
                   help="serve the full Copilot catalog; default: only the three Claude 5.5 models "
                        "(claude-sonnet-5.5, claude-opus-5.5, claude-haiku-5.5), every request mapped by family")
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
    s.add_argument("--context-edit", choices=CONTEXT_EDIT_MODES,
                   default=os.environ.get("COPILOT_PROXY_CONTEXT_EDIT", "tools"),
                   help="server-side context editing on the native Claude route, as the official Copilot client "
                        "does: tools = clear old tool results once a request passes COPILOT_PROXY_CLEAR_TRIGGER "
                        "(100000) input tokens, in steps of COPILOT_PROXY_CLEAR_STEP (10) tool uses so the prompt "
                        "cache survives between steps, keeping at least COPILOT_PROXY_CLEAR_KEEP (3); thinking = "
                        "drop old thinking blocks (needs thinking on); both; off")
    s.add_argument("--unix-socket", default=os.environ.get("COPILOT_PROXY_UNIX_SOCKET"),
                   help="listen on this Unix socket (mode 0600) instead of TCP; Claude Code connects with "
                        "ANTHROPIC_UNIX_SOCKET. No port for other processes or sandboxed commands to reach")
    s.add_argument("--auth-token", default=os.environ.get("COPILOT_PROXY_AUTH_TOKEN"),
                   help="require this client key (Authorization: Bearer or x-api-key) on every route except "
                        "/health; 'auto' generates one and prints it in the Claude Code env")
    s.add_argument("--sandbox", action="store_true",
                   help="with -c: also print the Claude Code sandbox block that hides the GitHub login from "
                        "sandboxed commands")
    s.add_argument("--sandbox-self", action="store_true",
                   help="macOS: run the proxy under sandbox-exec with no access to your home directory except its "
                        "own files")
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
    try:
        args.func(args)
    except (UpstreamError, OSError) as exc:  # no network, login refused, port in use: one line, no traceback
        LOG.debug("failed", exc_info=True)
        raise SystemExit(f"copilot_proxy: {exc}")


if __name__ == "__main__":
    main()
