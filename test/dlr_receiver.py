#!/usr/bin/env python3
"""
dlr_receiver.py — High-throughput DLR callback receiver for Kannel
Listens on port 8080, accepts GET /dlr?id=...&status=...

Architecture:
  HTTP handler  →  asyncio.Queue  →  background flusher  →  Redis XADD pipeline
  (returns OK immediately, never blocks on Redis)

This decouples HTTP response latency from Redis write latency.
The flusher batches up to FLUSH_SIZE entries or fires every FLUSH_MS ms,
whichever comes first — giving ~10x throughput vs per-request XADD.

Escape codes from urltrans_fill_escape_codes (urltrans.c):
  %I → internal UUID      %F → foreign_id
  %i → smsc-id            %p → receiver MSISDN
  %P → sender MSISDN      %d → dlr_mask
  %o → account field
"""

import asyncio
import json
import time
import sys
from aiohttp import web

try:
    import redis.asyncio as aioredis
except ImportError:
    print("ERROR: pip install redis", file=sys.stderr)
    sys.exit(1)

REDIS_URL  = "redis://localhost"
REDIS_DB   = 2
STREAM_KEY = "dlr_stream"
PORT       = 8080

FLUSH_SIZE = 200    # max entries per pipeline flush
FLUSH_MS   = 5      # max ms to wait before flushing a partial batch

# ── stats ─────────────────────────────────────────────────────
_received   = 0
_flushed    = 0
_start_ts   = time.time()


async def handle_dlr(request: web.Request) -> web.Response:
    global _received
    p = request.rel_url.query
    record = {
        "id":     p.get("id",     ""),
        "fid":    p.get("fid",    ""),
        "status": p.get("st",     p.get("status", "")),
        "smsc":   p.get("smsc",   ""),
        "from":   p.get("from",   ""),
        "to":     p.get("to",     ""),
        "acct":   p.get("acct",   ""),
        "ts":     str(int(time.time())),
    }
    # Non-blocking put — handler returns immediately
    request.app["queue"].put_nowait(record)
    _received += 1
    return web.Response(text="OK")


async def handle_stats(request: web.Request) -> web.Response:
    elapsed = time.time() - _start_ts
    return web.Response(
        content_type="application/json",
        text=json.dumps({
            "received":  _received,
            "flushed":   _flushed,
            "queued":    request.app["queue"].qsize(),
            "tps":       round(_received / elapsed, 1) if elapsed > 0 else 0,
            "elapsed":   round(elapsed, 1),
        })
    )


async def pipeline_flusher(app: web.Application) -> None:
    """Background task: drains the queue into Redis using pipelined XADD."""
    global _flushed
    q: asyncio.Queue = app["queue"]
    redis = app["redis"]
    flush_interval = FLUSH_MS / 1000.0

    while True:
        batch = []

        # Wait for the first item (blocks until something arrives)
        try:
            item = await asyncio.wait_for(q.get(), timeout=flush_interval)
            batch.append(item)
        except asyncio.TimeoutError:
            continue

        # Drain up to FLUSH_SIZE-1 more without blocking
        while len(batch) < FLUSH_SIZE:
            try:
                batch.append(q.get_nowait())
            except asyncio.QueueEmpty:
                break

        # Pipeline all XADDs in one round-trip
        pipe = redis.pipeline(transaction=False)
        for record in batch:
            pipe.xadd(STREAM_KEY, record)
        await pipe.execute()

        _flushed += len(batch)
        if _flushed % 5000 == 0 or (_flushed <= 1000 and _flushed % 1000 == 0):
            elapsed = time.time() - _start_ts
            tps_recv  = _received / elapsed if elapsed > 0 else 0
            tps_flush = _flushed  / elapsed if elapsed > 0 else 0
            print(
                f"[dlr_receiver] received={_received}  flushed={_flushed}"
                f"  recv_tps={tps_recv:.0f}/s  flush_tps={tps_flush:.0f}/s",
                flush=True,
            )


async def on_startup(app: web.Application) -> None:
    app["queue"] = asyncio.Queue(maxsize=0)  # unbounded
    app["redis"] = await aioredis.from_url(
        REDIS_URL, db=REDIS_DB, decode_responses=True,
        max_connections=4,
    )
    app["flusher"] = asyncio.ensure_future(pipeline_flusher(app))
    print(f"[dlr_receiver] Redis connected (DB {REDIS_DB})", flush=True)
    print(f"[dlr_receiver] Listening on port {PORT}  flush_size={FLUSH_SIZE}  flush_ms={FLUSH_MS}", flush=True)


async def on_cleanup(app: web.Application) -> None:
    app["flusher"].cancel()
    # Flush any remaining items
    q: asyncio.Queue = app["queue"]
    if not q.empty():
        pipe = app["redis"].pipeline(transaction=False)
        while not q.empty():
            pipe.xadd(STREAM_KEY, q.get_nowait())
        await pipe.execute()
    await app["redis"].aclose()


def build_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/dlr",   handle_dlr)
    app.router.add_get("/stats", handle_stats)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT, access_log=None)
