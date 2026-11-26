# Security

This file says what the proxy protects, who can reach it, how to run it safely, what a scan of the code found, and how
to check all of it yourself. The short list is in the [README](README.md#security).

## What is at stake

| Asset | Why it matters |
|---|---|
| Your GitHub login for Copilot (the saved token, or the editor login in `~/.config/github-copilot`) | Whoever reads it can use Copilot as you until you revoke it |
| Your Copilot account | Every prompt spends premium requests or tokens; heavy or strange use can get the account suspended |
| The short-lived Copilot token | Same as the login, for about a day |
| Your prompts and code | They pass through the proxy to GitHub Copilot over TLS; the proxy stores none of it |

## Who can reach the proxy

The proxy listens on `127.0.0.1:4141` by default. That keeps the network out, but not everything on your machine.

| Who | What they can do with the defaults | What stops them | What you can add |
|---|---|---|---|
| A website open in your browser | Nothing | Requests with a foreign `Host` (DNS rebinding), with an `Origin`, or marked by the browser as coming from another site get 403. No CORS header is ever sent unless you ask for one | Nothing needed. Don't use `--cors-origin '*'` |
| A program you run on the same machine, including every command an AI agent runs, MCP servers and hooks | Send prompts through the proxy and read `/usage` and `/stats`. It cannot get a token from the proxy. It can read the login files, like any program running as you | Nothing: on TCP there is no client key by default | `--unix-socket` or `--auth-token`, plus the sandbox settings below |
| Another user of the same machine | The same as a local program, because `127.0.0.1` is shared by all users. The proxy's own saved login is not readable by them (mode 0600 in a 0700 directory); an editor login in `~/.config/github-copilot` has the mode its editor plug-in gave it | File permissions only | `--unix-socket` (the socket is mode 0600) or `--auth-token`; `chmod 700 ~/.config/github-copilot` and `chmod 600` its files |
| Someone on your network | Nothing | The proxy only listens on the loopback address. Any other `--host` is refused unless a client key is set | Use an SSH tunnel instead of another host; see below |
| Someone between you and GitHub | Nothing | TLS with certificate checks on every upstream call; the API address GitHub returns is used only when it is `https` on `*.githubcopilot.com` or `*.ghe.com` | Nothing needed |
| GitHub Copilot | Sees everything you send | - | This is an unofficial client; see [What stays a risk](#what-stays-a-risk) |

A prompt injection is a "program you run": if a model is tricked into running a command, that command has the rights of
the second row. This is why the sandbox setup matters when an agent works on files or pages you did not write.

## How to run it

### 1. Default: one person, one laptop

```bash
python3 copilot_proxy.py start -c
```

- Safe against websites and the network.
- Any local program can use the account. Fine if you trust what runs on the machine; not enough if an agent runs
  commands from untrusted input.

### 2. Recommended with coding agents: Unix socket and sandbox

```bash
python3 copilot_proxy.py start --unix-socket ~/.local/share/copilot-api/proxy.sock -c --sandbox --sandbox-self
```

| Flag | What it adds |
|---|---|
| `--unix-socket PATH` | No TCP port at all. The socket file is mode 0600 in a 0700 directory, so only your user can connect, and Claude Code's sandboxed commands cannot reach it unless you list it in `sandbox.network.allowUnixSockets` (don't) |
| `-c --sandbox` | Prints Claude Code settings that keep the login away from what the model runs: `sandbox.filesystem.denyRead` and `permissions.deny Read(...)` for the login files, `allowUnsandboxedCommands: false`, and `CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1` so commands do not inherit credentials from the environment |
| `--sandbox-self` (macOS) | Runs the proxy itself under `sandbox-exec`: no access to your home directory except its own script, login, log and socket, and no permission to start other programs |

Notes:
- `ANTHROPIC_UNIX_SOCKET`, `ANTHROPIC_BASE_URL` and `ANTHROPIC_AUTH_TOKEN` must be exported in the shell that starts
  Claude Code; from `settings.json` they apply too late. `-c` prints the `export` line.
- The sandbox settings change how Claude Code runs every command (network and file access). Read what `-c --sandbox`
  prints before you put it into `.claude/settings.json`.
- The proxy refuses `--sandbox-self` if its token file, socket or log would sit directly in the home directory.

### 3. TCP with a client key

```bash
COPILOT_PROXY_AUTH_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')" \
  python3 copilot_proxy.py start -c
# or: python3 copilot_proxy.py start --auth-token auto -c     (generates a key and prints it)
```

- Every route except `/health` (and `/`, which gives the same answer) then needs the key as
  `Authorization: Bearer KEY` or `x-api-key: KEY`. Claude Code sends it when `ANTHROPIC_AUTH_TOKEN` holds the key;
  `-c` prints that.
- Prefer the environment variable: a key given as `--auth-token KEY` is visible to other users in `ps`.
- The printed `settings.json` then contains the key. Keep that file out of git.
- Without the sandbox, the key in the environment also reaches the commands an agent runs.
  `CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1` removes it there.

### 4. A machine with several users

Use setup 2 or 3. Never use `--show-token` there, and don't pass the login with `-g TOKEN` (visible in `ps`): let the
proxy use the saved login.

### 5. Reaching the proxy from another machine or a container

Prefer an SSH tunnel to a proxy that still listens on the loopback address:

```bash
ssh -N -L 4141:127.0.0.1:4141 you@host-that-runs-the-proxy
```

If you must listen on another address (`-H 0.0.0.0` or a LAN address):
- the proxy starts only with a client key (`--auth-token`). On `0.0.0.0` or `::` it then accepts any `Host`; on one
  address it accepts that address and the names you add with `--allow-host`;
- the traffic between client and proxy is plain HTTP: the key and your prompts cross the network unencrypted. Put a
  TLS-terminating server in front, or stay on a network you control;
- the proxy answers 400 to a request that carries both `Content-Length` and `Transfer-Encoding`, so a server in front
  cannot disagree with it about where a request ends.

### 6. Behind a company HTTP proxy

`HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY` are honoured. The connection to GitHub stays end-to-end TLS (CONNECT tunnel).
A password in `http://user:pass@proxy:port` is sent to that proxy in clear text, as with any HTTP proxy. A `socks://`
proxy is refused rather than bypassed.

## The controls

| Control | Default | Switch |
|---|---|---|
| Listen address | `127.0.0.1` | `-H`; anything but a loopback address needs `--auth-token` |
| `Host` check (DNS rebinding) | Only `127.0.0.1`, `localhost`, `[::1]` and the listen address | `--allow-host NAME` |
| Browser requests | Refused: any `Origin`, and any `Sec-Fetch-Site: cross-site` or `same-site`; no CORS headers | `--cors-origin ORIGIN` |
| Client key | None on TCP | `--auth-token KEY\|auto`, env `COPILOT_PROXY_AUTH_TOKEN`; compared in constant time |
| Unix socket | Off | `--unix-socket PATH` (mode 0600; an existing file that is not a socket is never replaced) |
| `GET /token` | Off | `--show-token` and a client key (or the Unix socket). `--show-token` also writes tokens to the log |
| `GET /health` | Open; shows the API address only when no client key is set | - |
| Silent clients | Disconnected after 120 s without request data; a stalled body gets 408 | env `COPILOT_PROXY_CLIENT_TIMEOUT` (seconds) |
| Request size | 64 MB body, 2,000,000 JSON items, 100 headers, 64 KB per header line | - |
| Request framing | `Content-Length` with `Transfer-Encoding`, bare line feeds in a chunked body, unknown transfer codings: 400 | - |
| Text from clients on your terminal | Control characters, line breaks and text-direction marks are shown as escapes in the log and in the `--manual` prompt | - |
| What goes upstream | The client's `Authorization`, `x-api-key` and cookies are never forwarded; only `anthropic-version` and `anthropic-beta` are | env `COPILOT_PROXY_DROP_BETA=1` drops the betas |
| Upstream TLS | Certificates are always checked; there is no switch to turn that off | - |
| Upstream address | From GitHub's token answer, only `https` on `*.githubcopilot.com` or `*.ghe.com`; otherwise the built-in address stays | `-a individual\|business\|enterprise` picks the built-in one |
| Saved login | Written atomically, mode 0600, in a 0700 directory | `--token-file` |
| Usage log | Off; a new file is mode 0600 | `--usage-log FILE` |
| Approve each request by hand | Off | `--manual` |
| Slow down | Off | `-r SECONDS`, `-w` |

## What the proxy keeps

| Where | What | What never |
|---|---|---|
| Saved login file | The GitHub OAuth token from the device login | - |
| Usage log (only with `--usage-log`) | Per request: time, route, model, initiator, status, token counts, duration | Prompt or answer text, tokens |
| Log on the terminal | Request lines, model mapping, token counts, the first 500 bytes of a Copilot error (which can quote part of a request), the 4-character type prefix of an editor login it tries | Tokens, unless you pass `--show-token` |
| Memory | The Copilot token; a request while it is being forwarded | Nothing is written to disk |

## Do and don't

Do:
- Keep the default listen address. For other machines use an SSH tunnel.
- Use `--unix-socket` or `--auth-token` whenever an agent runs commands on the machine.
- Restart the proxy after you update the file; a running copy keeps its old code.
- Stop copies you no longer use and check that only one listens: `lsof -nP -iTCP:4141 -sTCP:LISTEN`.
- Watch your quota with `python3 copilot_proxy.py check-usage` and the totals at `GET /stats`.
- Keep a `settings.json` that contains the client key out of git.
- Check who can read the editor login: `ls -l ~/.config/github-copilot`. Make it yours only if it is not.
- Revoke the login at <https://github.com/settings/applications> if a login file may have been read.

Don't:
- `--cors-origin '*'`: it lets every website use your account.
- `--show-token` on a shared machine or when the log goes to a file others can read.
- `-g TOKEN` or `--auth-token KEY` on the command line of a shared machine.
- List the proxy's socket or port in Claude Code's sandbox allow lists.
- Run a copy from before commit `6f22f41`; see the next section.

## Old copies answer every website

The first version of this file (before commit `6f22f41`, 24 Nov 2026) sent `Access-Control-Allow-Origin: *` and did
not check `Host` or `Origin`. A page open in a browser on the same machine could send prompts on the account and read
the answers. If such a copy is still running, stop it. To test a running proxy:

```bash
curl -si -X OPTIONS -H 'Origin: https://example.com' http://127.0.0.1:4141/v1/messages | grep -i access-control
```

Any output means an old copy. A current copy prints nothing (the request gets 403).

## Scan of 26 Nov 2026

Method: static analysis with bandit 1.9.4, a pattern scan of the whole git history, and 28 live probes against a local
instance with a fake Copilot. No high-severity issue was found in the code. Every finding below is closed except C1,
which is a documented choice.

| Measure | At the scan | After the fixes |
|---|---|---|
| Live probes that hold | 15 of 28 | 28 of 28 |
| Checks in `test_offline.py` | 98 | 119 (11 for the findings, 10 for the controls that held) |
| bandit | 0 high, 2 medium, 2 low | the same 4 notes; see [Check it yourself](#check-it-yourself) |
| Secrets in the git history (13 patterns) | 0 | 0 |

| Id | Severity | Finding | State | Check |
|---|---|---|---|---|
| C1 | Medium, by design | On TCP there is no client key unless you pass `--auth-token`, so every local program can use the account | Kept: use setup 2 or 3. New: a host other than a loopback address is refused without a key | H10 |
| C2 | Low (medium with `--manual`) | Control characters from a client reached the terminal: escape sequences in the request line, a forged log line through a line break in the model name, and client-chosen text in the `--manual` prompt | Fixed in `f851061` | H1, H2 |
| C3 | Low on `127.0.0.1` | No timeout on client connections: 150 of 150 unfinished requests still held a thread | Fixed in `f851061` (120 s, 408 for a stalled body) | H3 |
| C4 | Low | A 16 MB body of empty lists took 424 MB of memory | Fixed in `f851061` (limit of 2,000,000 JSON items, checked before parsing) | H4 |
| C5 | Low | A cross-site browser GET without an `Origin` header reached `/usage`, `/v1/models` and `/stats`. The page could not read the answer or send a prompt | Fixed in `f851061` (`Sec-Fetch-Site`) | H5 |
| C6 | Low | With `--show-token`, `GET /token` needed no client key | Fixed in `f851061` | H6 |
| C7 | Low | `Content-Length` together with `Transfer-Encoding`, and bare line feeds in a chunked body, were accepted | Fixed in `f851061` (400) | H7 |
| C8 | Low | `/health` showed the Copilot API address without a key | Fixed in `f851061` (hidden when a key is set) | H6 |
| C9 | Low | The API address in GitHub's token answer was used unchecked | Fixed in `f851061` | H8 |
| C10 | Info | The usage log was created world-readable | Fixed in `f851061` (0600) | H9 |
| C11 | Info | Listening on all interfaces without a key only printed a warning | Fixed in `f851061` (refused) | H10 |
| C12 | Info | `-g TOKEN` on the command line is visible in `ps` | Documented here | - |
| - | Info | A client that reset its connection printed a traceback on the terminal | Fixed in `cbab785` | H11 |

What held at the scan, and is now checked on every run (`SC1` to `SC10`):
- 11 foreign or malformed `Host` values, a missing `Host` and a foreign first of two `Host` headers: all 403.
- A browser POST with a foreign, `null` or even the proxy's own `Origin`: 403, and nothing reached Copilot.
- A missing, wrong, empty, `Basic` or in-the-URL client key: 401.
- 150 headers, a 70 KB header and a 70 KB request line: refused. A request pipelined behind a refused one: not processed.
- A folded header added nothing upstream; the client's own credentials were not forwarded.
- A path with quotes could not add a rule to the `--sandbox-self` profile.
- No token in the log at debug level.
- An upstream with a self-signed certificate was refused and received nothing.

Not covered by the scan: fuzzing beyond these probes, Windows and Linux, the `bwrap` line in the README, how each
browser's own local-network rules treat requests to `127.0.0.1`, and the Copilot service itself.

## Check it yourself

```bash
python3 test_offline.py security hardening   # the 21 security checks; no network, login or quota needed
python3 test_offline.py                      # all 119 checks
pip install bandit && bandit copilot_proxy.py
```

bandit reports 4 notes, none of them a defect:

| Note | Why it is not a defect |
|---|---|
| B310 `urlopen` | The URLs are constants on `https://github.com` and `https://api.github.com`, or the checked Copilot API address (C9) |
| B104 bind to all interfaces | The line compares the listen address with `0.0.0.0`; it does not bind to it |
| B606 process without a shell | `--sandbox-self` re-executes the interpreter under `sandbox-exec` with a fixed argument list |
| B105 hard-coded password | The string is the option value `auto` |

Against a running proxy (replace the port if you changed it):

| Command | Expected |
|---|---|
| `curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: evil.example' http://127.0.0.1:4141/v1/models` | `403` |
| `curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'Origin: https://evil.example' -d '{}' http://127.0.0.1:4141/v1/messages` | `403` |
| `curl -s -o /dev/null -w '%{http_code}\n' -H 'Sec-Fetch-Site: cross-site' http://127.0.0.1:4141/stats` | `403` |
| `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:4141/token` | `403` |
| `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:4141/stats` with a client key set | `401` |

None of these calls reaches Copilot.

## What stays a risk

- **No client key by default (C1).** Local programs can use the account until you choose setup 2 or 3.
- **Unofficial client.** The proxy presents itself as VS Code Copilot Chat and uses Copilot's internal endpoints. GitHub
  does not support this. Heavy or automated use can trigger abuse detection and get Copilot access suspended. `-r` and
  `--manual` slow things down.
- **Cost.** With `--any-model`, a model name the account does not list falls back to a Sonnet model, and Opus costs
  several times more than Sonnet. Pin the models with `-m` and `-s`, set `--subagent-model`, and watch `GET /stats`.
- **Local denial of service.** A local program can still open many connections (each is dropped after the client
  timeout) or send bodies up to the limits.
- **Rare duplicate request.** If Copilot closes a kept connection at the moment a request is sent, the proxy sends
  that request once more on a new connection.
- **Other platforms.** `--sandbox-self` is macOS only. The Linux `bwrap` line in the README is not tested.

## Reporting a problem

Please do not put details of a vulnerability in a public issue.
- If the repository's **Security** tab shows **Report a vulnerability**, use it.
- Otherwise open an issue that only says you have a security report, and wait to be contacted.

Include the commit you tested, the command line of the proxy, and the request that shows the problem.
