# pi-server Protocol

pi-server is an HTTP/SSE bridge around `@earendil-works/pi-coding-agent`.
One process hosts many agents. A client routes each request by sending an agent id.

## Directory layout

Given `cwd = process.cwd()`:

```text
cwd/
  agents/
    <agent-id>/
      settings.json
      SYSTEM.md
      auth.json        # optional/provider dependent
      models.json      # optional custom models
  sessions/
    <agent-id>.jsonl   # dynamic pi conversation history
```

`agents/<agent-id>/` is static configuration. `sessions/` is dynamic state.
Unknown agent ids return `401`.

## Agent id

The agent id is a long-lived routing credential generated outside pi-server. It must:

- contain only letters, numbers, `.`, `_`, `-`
- start and end with a letter or number
- match an existing `agents/<agent-id>/` directory

Send it as either:

```http
Authorization: Bearer <agent-id>
```

or:

```http
X-Agent-Id: <agent-id>
```

## Endpoints

### `GET /health`

No agent id required.

Returns server-level status:

```json
{
  "status": "ok",
  "root": "/var/lib/pi-server",
  "agentsDir": "/var/lib/pi-server/agents",
  "sessionsDir": "/var/lib/pi-server/sessions",
  "loadedAgents": 2
}
```

### `GET /agent`

Requires agent id. Returns status for that loaded or loadable agent.

```json
{
  "agentId": "demo-agent",
  "agentDir": "/var/lib/pi-server/agents/demo-agent",
  "sessionPath": "/var/lib/pi-server/sessions/demo-agent.jsonl",
  "sessionId": "demo-agent",
  "messageCount": 12,
  "model": "anthropic/claude-sonnet-4-5",
  "thinkingLevel": "off",
  "isStreaming": false
}
```

### `POST /chat`

Requires agent id and `Content-Type: application/json`.

Request body:

```json
{
  "prompt": "hello"
}
```

Response is Server-Sent Events:

```text
event: text_delta
data: {"delta":"Hi"}

event: agent_end
data: {}
```

Possible events:

- `text_delta`
- `thinking_start`
- `thinking_delta`
- `thinking_end`
- `agent_end`
- `error`
- `:heartbeat` every 15 seconds

## Concurrency and abort

Multiple agents can stream concurrently.

Within one agent, a newer `/chat` request preempts the current generation:

- old SSE response receives `event: error` with `superseded by newer request`
- old generation is aborted
- new prompt starts after the abort completes

If the HTTP response body is closed by the client, pi-server aborts the active generation for that agent.

## Settings precedence

Before each prompt, pi-server reloads the agent and applies `agents/<agent-id>/settings.json` as the source of truth for:

- `defaultProvider`
- `defaultModel`
- `defaultThinkingLevel`

These settings override any persisted `model_change` or `thinking_level_change` entries in `sessions/<agent-id>.jsonl`.
