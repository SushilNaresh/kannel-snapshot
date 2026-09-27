#!/usr/bin/env python3
"""
send_mts.py — parallel sendsms HTTP sender for TPS benchmark
Usage: python3 test/send_mts.py <count> <concurrency> [port1,port2,...]
Default ports: 13013
Prints summary line to stderr: accepted=N rejected=N errors=N elapsed=Xs tps=N
"""
import sys
import time
import asyncio
import aiohttp

COUNT       = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
CONCURRENCY = int(sys.argv[2]) if len(sys.argv) > 2 else 50
PORTS       = [int(p) for p in sys.argv[3].split(",")] if len(sys.argv) > 3 else [13013]
DLR_URL     = "http://127.0.0.1:8080/dlr?id=%I&st=%d"

accepted = 0
rejected = 0
errors   = 0

async def send_one(session: aiohttp.ClientSession, i: int) -> None:
    global accepted, rejected, errors
    port = PORTS[i % len(PORTS)]
    params = {
        "username": "foo",
        "password": "bar",
        "to":       f"3585{i}",
        "text":     "T",
        "dlr-url":  DLR_URL,
        "dlr-mask": "31",
        "account":  "bench",
    }
    url = f"http://127.0.0.1:{port}/cgi-bin/sendsms"
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            body = await resp.text()
            if body.startswith("0:"):
                accepted += 1
            else:
                rejected += 1
                print(f"REJECT {i} port={port}: {body.strip()}", file=sys.stderr)
    except Exception as e:
        errors += 1
        print(f"ERROR {i} port={port}: {e}", file=sys.stderr)

async def main() -> None:
    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY + len(PORTS))

    async def bounded(i: int) -> None:
        async with sem:
            await send_one(session, i)

    start = time.time()
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(*[bounded(i) for i in range(1, COUNT + 1)])
    elapsed = time.time() - start

    tps = int(accepted / elapsed) if elapsed > 0 else 0
    print(f"accepted={accepted} rejected={rejected} errors={errors} "
          f"elapsed={elapsed:.1f}s tps={tps}", file=sys.stderr)

asyncio.run(main())
