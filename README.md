# copilot-proxy-py

A single-file proxy that uses only the Python standard library. It lets the **Claude Code CLI** (and any
OpenAI-compatible client) use your **GitHub Copilot** subscription. It's a Python port of the design of
[ericc-ch/copilot-api](https://github.com/ericc-ch/copilot-api).

- One file, `copilot_proxy.py`, Python 3.9+, no `pip install`
- Anthropic Messages API to OpenAI Chat Completions, with or without streaming, including tools and images
- Automatic Copilot token refresh, live model list, and mapping of Claude model names
  (`claude-sonnet-4-6-20260101` → `claude-sonnet-4.6`)

> **Warning.** This is an unofficial, reverse-engineered client. It presents itself as VS Code Copilot Chat and uses
> Copilot's internal endpoints, which is not supported by GitHub. Heavy or automated use may trigger GitHub's abuse
> detection and get your Copilot access suspended. Use it at your own risk; the rate limit (`-r`) and the per-request
> approval prompt (`--manual`) help.

## Quick start

```bash
git clone https://github.com/phungmanhcuong/copilot-proxy-py.git
cd copilot-proxy-py
python3 copilot_proxy.py start -c
```

On the first run, if no login is found, it prints a GitHub device code. Open https://github.com/login/device,
enter the code, and approve. The token is saved to `~/.local/share/copilot-api/github_token` (mode 0600) and reused
after that.

`-c` prints the exact Claude Code environment, built from the models your plan offers. In a second terminal:

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:4141 \
ANTHROPIC_AUTH_TOKEN=dummy \
ANTHROPIC_MODEL=claude-sonnet-4.6 \
ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-haiku-4.5 \
CLAUDE_CODE_SUBAGENT_MODEL=claude-haiku-4.5 \
CLAUDE_CODE_AUTO_COMPACT_WINDOW=200000 \
CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
claude
```

**Default models.** The defaults are the Claude 5.5 family: `claude-sonnet-5.5` (main), `claude-opus-5.5` and
`claude-haiku-5.5` (subagents and background calls).
- A family whose 5.5 model your account's `/models` doesn't list falls back to the newest version it does list.
  For example, legacy annual plans get no new models, so they stay on `claude-sonnet-4.6`, `claude-opus-4.8` and
  `claude-haiku-4.5`.
- Change the defaults with `COPILOT_PROXY_DEFAULT_SONNET`, `COPILOT_PROXY_DEFAULT_OPUS` and
  `COPILOT_PROXY_DEFAULT_HAIKU`, or with `-m` / `-s`.

Use the model ids that `start -c` prints for your account. You can also put the same variables under `"env"` in
`.claude/settings.json` (measured with TCP). In Unix-socket mode, the connection variables must be in the shell; see
[Sandbox mode](#sandbox-mode).

## Login order
The proxy uses the first of these that works:
1. `-g <github-oauth-token>`
2. the saved token file (`--token-file`, default `~/.local/share/copilot-api/github_token`)
3. an existing Copilot editor login in `~/.config/github-copilot/apps.json` (skip with `--no-copilot-config`)
4. `GH_TOKEN` / `GITHUB_TOKEN`, only with `--use-env-token`
5. the GitHub device login (also available on its own: `python3 copilot_proxy.py auth`)

Tokens are never logged unless you pass `--show-token`.

## Commands and options

```bash
python3 copilot_proxy.py start [-p 4141] [-H 127.0.0.1] [-a individual|business|enterprise]
                               [-m MAIN_MODEL] [-s SMALL_MODEL] [-r SECONDS] [-w] [--manual] [-c] [-v]
python3 copilot_proxy.py check-usage      # plan and premium-request quota
python3 copilot_proxy.py auth             # GitHub device login only
```

| Option | Meaning |
|---|---|
| `-r 5 -w` | at most one request per 5 s; with `-w` it waits instead of returning 429 |
| `--manual` | ask y/N in the terminal before each request is forwarded |
| `-m`, `-s` | force the model ids used for normal and haiku-class requests |
| `-a` | account type for Copilot Business or Enterprise |
| `--unix-socket PATH` | listen on a Unix socket (mode 0600) instead of TCP; see [Sandbox mode](#sandbox-mode) |
| `--auth-token KEY\|auto` | require a client key on every route except `/health`; `auto` generates one and prints it in the `-c` env. Prefer env `COPILOT_PROXY_AUTH_TOKEN` over the flag (command lines are visible in `ps`) |
| `--context-edit tools\|thinking\|both\|off` | server-side context editing on the native Claude route (default `tools`) |
| `--sandbox` | with `-c`: also print the Claude Code sandbox and permission settings |
| `--sandbox-self` | macOS: run the proxy itself under `sandbox-exec` |

The proxy honours `HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY` for all upstream calls, streaming included (HTTP CONNECT
tunnel; `http://user:pass@host:port` works).

## Token and premium-request saving

Copilot bills in one of two ways:
- **Legacy annual plans** pay **premium requests**: one per prompt *you* send, multiplied by the model's rate
  (Haiku 4.5 = 0.33, Sonnet 4.6 = 9, Opus 4.8 = 27). Tool-call follow-ups are free.
- **Usage-based plans** (since June 2026) pay **per token**, and a cached input token costs a tenth of an uncached one.

The proxy cuts both:

| What | Switch | Measured (Claude Code 2.1.330, same 2–3 tool-turn task) |
|---|---|---|
| **Native `/v1/messages` pass-through** for models that list it (Claude). It forwards the request as is, so Claude Code's `cache_control` breakpoints, thinking blocks and signatures survive. Everything else is translated to `/chat/completions`. If Copilot rejects a native request, the proxy falls back to translation. | `--mode auto` (default), `native`, `translate`; env `COPILOT_PROXY_MODE` | Warm runs: **18–26** uncached input tokens native against **18,022–18,238** translated; cache hit 99.0–99.1% against 73.6–80.1%; at Copilot's Haiku token prices the warm run costs about 64% less. The first run of a session pays a one-time cache write. All answers correct (8 runs). |
| **Server-side context editing** (`clear_tool_uses_20250919`, as the official Copilot client sends it): once a request is over 100,000 input tokens, Copilot replaces old tool results with a placeholder. The proxy adds it next to Claude Code's own `context_management` and the needed beta. It clears in **steps of 10 tool uses** (keeping at least the last 3), so the cleared part, and the prompt cache, stay unchanged between steps. If Copilot rejects the addition, the request is re-sent as the client sent it. | `--context-edit` (default `tools`); env `COPILOT_PROXY_CLEAR_TRIGGER` (100000), `COPILOT_PROXY_CLEAR_STEP` (10), `COPILOT_PROXY_CLEAR_KEEP` (3) | One request with 4 tool results: 20,032 → 5,656 input tokens, answer correct. Growing tool loop (trigger lowered to 5,000): stepped clearing sent 11.6k–15.2k cached tokens per turn instead of 28.6k–32.2k and wrote only the new 1,774 tokens per turn; a full cache rewrite happens only every 10 tool uses. A fixed `keep: 3` (the official client's setting) rewrote the whole cache on every turn. Long sessions also reach Claude Code's auto-compaction (a new, billed prompt) later. |
| **Exact `count_tokens`** from Copilot's native endpoint, so Claude Code compacts at the right time | `--count-tokens auto` (default), `native`, `estimate` | native count = real input tokens on 5 of 5 requests; the old local estimate was off by 25.5% on average (−52% with tool definitions) |
| **Agent follow-ups marked as agent:** turns that only carry tool results are sent with `X-Initiator: agent` and are not billed as new prompts | always on | 4 Haiku task runs (12 model calls) cost 1 premium request (4 × 0.33) |
| **Subagents on the cheap model:** `start -c` prints `CLAUDE_CODE_SUBAGENT_MODEL` = the Haiku model, because every subagent starts with a fresh, billed prompt | `--subagent-model` | 0.33 instead of 9 premium requests per subagent on legacy plans |
| **Compaction window = Copilot's prompt limit:** `start -c` prints `CLAUDE_CODE_AUTO_COMPACT_WINDOW` from `/models` (200,000 for Sonnet 4.6), so a long session compacts before Copilot rejects it | printed env | – |
| **Output clamp:** `max_tokens` is capped at the model's output limit, and `thinking.budget_tokens` is kept below it, so a request is never rejected and re-sent | always on | – |
| **`Retry-After` passed through:** on a Copilot 429/5xx the proxy forwards `retry-after` and the request id, so Claude Code's own retry waits the right time. Optional retries inside the proxy, before streaming, are for other clients; a rejected 429/5xx costs no tokens either way. | env `COPILOT_PROXY_RETRIES` (default 0) | – |
| **Usage log:** one JSON line per request with input, cache read, cache write and output tokens, cleared tokens, route, model and initiator; totals and cache-hit ratio at `GET /stats` | `--usage-log FILE`, env `COPILOT_PROXY_USAGE_LOG` | – |

Claude Code settings that also cut tokens (all exist in Claude Code 2.1.330):
- `MAX_MCP_OUTPUT_TOKENS` caps large MCP tool outputs.
- `ENABLE_TOOL_SEARCH` loads MCP tool definitions only when they are needed.
- `CLAUDE_CODE_EFFORT_LEVEL` and `MAX_THINKING_TOKENS` limit thinking.
- `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` makes compaction start earlier.

Don't set `DISABLE_PROMPT_CACHING`.

## Copilot API version

The proxy presents itself as the current stable Copilot client: **VS Code 1.147.0 with Copilot Chat 0.75.0**.
Copilot Chat now ships inside VS Code; the standalone extension repo was archived in May 2026. The values come from
`microsoft/vscode` at tag `1.147.0`, `extensions/copilot`.

| Header | Value | Override |
|---|---|---|
| `Editor-Version` | `vscode/1.147.0` | `COPILOT_PROXY_VSCODE_VERSION` |
| `Editor-Plugin-Version`, `User-Agent` | `copilot-chat/0.75.0`, `GitHubCopilotChat/0.75.0` | `COPILOT_PROXY_CHAT_VERSION` |
| `X-GitHub-Api-Version` on Copilot API calls | `2026-08-01` (set by the `@vscode/copilot-api` 0.5.2 package the extension uses; `networking.ts` alone says 2026-01-09) | `COPILOT_PROXY_COPILOT_API_VERSION` |
| `X-GitHub-Api-Version` for the token exchange and quota on api.github.com | `2025-04-01` | `COPILOT_PROXY_GITHUB_API_VERSION` |
| `OpenAI-Intent`, `X-Interaction-Type` | `conversation-panel` | – |

Copilot rejects an unknown API version (`400 invalid apiVersion`), and it accepted 2026-08-01 on every route in the
final test. If a future Copilot release rejects these values, set the overrides to the new client's values.

## Sandbox mode

Claude Code's sandbox (`/sandbox`, `"sandbox": {"enabled": true}`) wraps the commands the model runs (Bash,
PowerShell, Monitor). Claude Code's own model calls run outside it, so the proxy works unchanged. Two defaults matter:
- sandboxed commands can **read the whole disk**, including the proxy's GitHub login;
- the built-in Read tool runs **outside** the sandbox.

The best setup:

```bash
python3 copilot_proxy.py start --unix-socket ~/.local/share/copilot-api/proxy.sock -c --sandbox --sandbox-self
```

- `--unix-socket`: no TCP port. Claude Code connects through `ANTHROPIC_UNIX_SOCKET`. The socket is mode 0600 in a
  0700 directory, so other users can't connect. Sandboxed commands can't connect either, unless you list it in
  `sandbox.network.allowUnixSockets` (don't).
  - `-c` prints an `export` line with `ANTHROPIC_UNIX_SOCKET`, `ANTHROPIC_BASE_URL` and `ANTHROPIC_AUTH_TOKEN`.
    These three must be in the **shell environment**. From `settings.json` `env` they apply too late in Claude Code
    2.1.330 (measured: connection refused, or TLS on the socket). The rest of the settings can go in
    `settings.json`.
  - macOS limits socket paths to 104 bytes; the default location above fits.
- `-c --sandbox` prints settings with:
  - `sandbox.filesystem.denyRead` for the login files;
  - `permissions.deny` `Read(//…)` rules for the Read tool;
  - `allowUnsandboxedCommands: false`;
  - `CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1`, so Bash, hooks and MCP servers don't inherit credentials.
- `--sandbox-self` (macOS) runs the proxy under `sandbox-exec`. It can't read or write the home directory except its
  own script, login, log and socket, and it can't start other programs.
  - On Linux, a similar `bwrap` line binds only what the proxy needs. It is not tested here, and a Python
    installed under `$HOME` (pyenv) also needs a `--ro-bind` for it:
    `bwrap --ro-bind / / --tmpfs $HOME --ro-bind ~/.config/github-copilot ~/.config/github-copilot --bind ~/.local/share/copilot-api ~/.local/share/copilot-api --ro-bind copilot_proxy.py /proxy.py python3 -I /proxy.py start ...`.

Measured with Claude Code 2.1.330 on macOS: one run used exactly the printed `export` line and settings, with the
proxy on Python 3.9 under `--sandbox-self`.

| Attempt from inside Claude Code | Result |
|---|---|
| sandboxed `cat` of a login file, `wc -c ~/.config/github-copilot/apps.json` | denied |
| sandboxed `curl --unix-socket` to the proxy | exit 7 (no connection) |
| sandboxed `echo $ANTHROPIC_AUTH_TOKEN` | `unset` (removed by the env scrub) |
| Read tool on the login file | denied by the `permissions.deny` rule |
| Grep tool on the login directory | denied (the Read rules cover Grep) |
| the task itself (1 prompt, 4 tool turns) | correct answer, prompt cache hits on every turn |

Under `--sandbox-self`, reading `~/.ssh`, `~/.zshrc` and `~/.aws` fails while the proxy serves requests. The proxy
refuses to start if its token file, socket or log sits directly in the home directory, because allowing that
directory would open all of it.

On TCP, use `--auth-token auto` if other local programs shouldn't use your account. Without the sandbox, a key in
the environment also reaches the commands Claude Code runs; `CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1` removes it there.

## Security
- The proxy binds to `127.0.0.1` by default.
- Requests whose `Host` isn't `127.0.0.1`, `localhost` or `[::1]` get 403. This blocks DNS rebinding. Add names
  with `--allow-host`.
- Browser requests that carry an `Origin` get 403, and no CORS headers are sent. This stops any web page from using
  your account. Allow an origin with `--cors-origin`.
- If you bind to `0.0.0.0`, anyone who can reach the port uses your Copilot account.

## Endpoints
- `POST /v1/messages`: Anthropic Messages, with or without streaming
- `POST /v1/messages/count_tokens`: exact count from Copilot for Claude models, otherwise a local estimate
- `POST /v1/chat/completions`: passed through to Copilot
- `POST /v1/responses`: passed through to Copilot, for `/responses`-only models such as `gpt-5.3-codex`
- `GET /stats`: token totals and cache-hit ratio since start
- `POST /v1/embeddings`: passed through to Copilot
- `GET /v1/models`
- `GET /usage`
- `GET /health`
- `GET /token`: disabled unless you pass `--show-token`

## Notes
- On the translated route (non-Claude models), Anthropic `thinking` blocks in the history are dropped, because the
  OpenAI format has no field for them. Anthropic server tools (for example `web_search`) are not forwarded. The
  native route keeps both.
- Follow-up turns in a tool loop are sent with `X-Initiator: agent`, as copilot-api does. Fresh prompts, including
  a subagent's first prompt, are sent as `user`: the proxy never relabels a new prompt to avoid billing.

## License
MIT. See `LICENSE`.
