#!/usr/bin/env python3
"""Small helper for agent-driven Cabana HTTP API workflows.

This script can:
  1) read message/signal metadata
  2) sample decoded signal values
  3) guess interesting (changing) signals
  4) ask Cabana to render charts for selected signals
"""

import argparse
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib import parse, request


@dataclass
class SignalTarget:
  source: int
  address: int
  name: str


def api_call(base_url: str, path: str, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
  req = request.Request(parse.urljoin(base_url, path), method=method)
  req.add_header("Content-Type", "application/json")
  data = json.dumps(payload).encode("utf-8") if payload is not None else None
  with request.urlopen(req, data=data, timeout=5) as resp:
    return json.loads(resp.read().decode("utf-8"))


def get_messages(base_url: str, source: int) -> list[dict[str, Any]]:
  out = api_call(base_url, f"/api/messages?source={source}")
  return out.get("messages", [])


def sample_signal(base_url: str, source: int, address: int, signal_name: str) -> float | None:
  q = parse.urlencode({"source": source, "address": hex(address), "signal": signal_name})
  out = api_call(base_url, f"/api/signals/value?{q}")
  return out.get("value") if out.get("decoded") else None


def guess_signals(base_url: str, source: int, sample_count: int, sample_period: float, top_n: int) -> list[SignalTarget]:
  messages = get_messages(base_url, source)
  candidates: list[SignalTarget] = []
  for msg in messages:
    for sig in msg.get("signals", []):
      candidates.append(SignalTarget(source=source, address=msg["address"], name=sig["name"]))

  scored: list[tuple[float, SignalTarget]] = []
  for c in candidates:
    vals = []
    for _ in range(sample_count):
      v = sample_signal(base_url, c.source, c.address, c.name)
      if v is not None:
        vals.append(v)
      time.sleep(sample_period)

    if len(vals) >= 2:
      score = max(vals) - min(vals)
      scored.append((score, c))

  scored.sort(key=lambda x: x[0], reverse=True)
  return [c for score, c in scored[:top_n] if score > 0]


def show_chart(base_url: str, target: SignalTarget, merge: bool) -> None:
  api_call(base_url, "/api/charts/show", method="POST", payload={
    "source": target.source,
    "address": target.address,
    "signal": target.name,
    "merge": merge,
  })


def main() -> None:
  p = argparse.ArgumentParser(description="Cabana HTTP API helper")
  p.add_argument("--base-url", default="http://127.0.0.1:8989")
  p.add_argument("--source", type=int, default=0)
  p.add_argument("--top-n", type=int, default=3)
  p.add_argument("--sample-count", type=int, default=4)
  p.add_argument("--sample-period", type=float, default=0.15)
  p.add_argument("--show", action="store_true", help="Render guessed signals as Cabana charts")
  args = p.parse_args()

  guessed = guess_signals(args.base_url, args.source, args.sample_count, args.sample_period, args.top_n)
  print("Guessed dynamic signals:")
  for g in guessed:
    print(f"  src={g.source} addr=0x{g.address:X} signal={g.name}")
    if args.show:
      show_chart(args.base_url, g, merge=True)


if __name__ == "__main__":
  main()
