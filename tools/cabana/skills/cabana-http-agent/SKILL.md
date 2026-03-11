# Cabana HTTP Agent Skill

Use this skill when you need to drive Cabana through HTTP for DBC signal read/write and quick chart visualization.

## What this skill controls

Cabana now exposes a local HTTP API (default: `http://127.0.0.1:8989`) with endpoints:

- `GET /health`
- `GET /api/messages?source=<bus>`
- `GET /api/signals/value?source=<bus>&address=<0xaddr>&signal=<name>`
- `POST /api/dbc/signal` (create/update in-memory DBC signal)
- `POST /api/charts/show` (plot a signal in charts view)

## Agent workflow

1. Ensure Cabana is running and has loaded a route/stream + DBC.
2. Query `GET /api/messages` to discover messages/signals.
3. For each target signal, use `GET /api/signals/value` to sample values.
4. Guess dynamic/interesting signals by value range or change rate.
5. Plot selected signals using `POST /api/charts/show`.
6. If signal mapping is wrong, patch the in-memory DBC with `POST /api/dbc/signal`.
7. Re-sample and verify decoded values and bit mapping are correct.

## Quick commands

```bash
# health check
curl -s http://127.0.0.1:8989/health | jq

# list source 0 messages
curl -s 'http://127.0.0.1:8989/api/messages?source=0' | jq

# read current decoded value for one signal
curl -s 'http://127.0.0.1:8989/api/signals/value?source=0&address=0x123&signal=STEER_ANGLE' | jq

# add/update a signal in memory
curl -s -X POST http://127.0.0.1:8989/api/dbc/signal \
  -H 'Content-Type: application/json' \
  -d '{"source":0,"address":291,"name":"TEMP_GUESS","start_bit":8,"size":8,"factor":1.0,"offset":-40,"is_little_endian":true,"is_signed":false}' | jq

# show chart
curl -s -X POST http://127.0.0.1:8989/api/charts/show \
  -H 'Content-Type: application/json' \
  -d '{"source":0,"address":291,"signal":"TEMP_GUESS","merge":true}' | jq
```

## Python helper

Use `tools/cabana/http/agent_client.py` to auto-sample, guess, and optionally plot dynamic signals:

```bash
python3 tools/cabana/http/agent_client.py --source 0 --top-n 5 --show
```

This is suitable for Codex/Claude automation loops where the agent iteratively updates bit definitions, re-reads decoded values, and plots confirmation charts.
