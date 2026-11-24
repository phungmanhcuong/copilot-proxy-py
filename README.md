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
git clone https://github.com/<you>/copilot-proxy-py.git
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
DISABLE_NON_ESSENTIAL_MODEL_CALLS=1 \
CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
claude
```

Use the model ids that `start -c` prints for your account. You can also put the same variables under `"env"` in
`.claude/settings.json`.

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

## Endpoints
- `POST /v1/messages`: Anthropic Messages, with or without streaming
- `POST /v1/messages/count_tokens`: an estimate, since the standard library has no tokenizer
- `POST /v1/chat/completions`: passed through to Copilot
- `POST /v1/embeddings`: passed through to Copilot
- `GET /v1/models`
- `GET /usage`
- `GET /health`
- `GET /token`: disabled unless you pass `--show-token`

The proxy binds to `127.0.0.1` by default. Don't expose it on a network: anyone who can reach it can use your
Copilot account.

## Notes
- Anthropic `thinking` blocks in the history are dropped, because the OpenAI format has no field for them.
- Anthropic server tools (for example `web_search`) are not forwarded.
- Follow-up turns in a tool loop are sent with `X-Initiator: agent`, as copilot-api does.

## License
MIT. See `LICENSE`.
