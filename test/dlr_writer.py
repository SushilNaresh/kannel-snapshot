#!/usr/bin/env python3
"""
dlr_writer.py — Async Redis Stream → MySQL batch writer
Reads dlr_stream from Redis DB 2, batch-inserts into dlr_report table.

DLR status values (from dlr.h):
  1  = DLR_SUCCESS      (delivered to handset)
  2  = DLR_FAIL         (failed)
  4  = DLR_BUFFERED     (buffered at SMSC)
  8  = DLR_SMSC_SUCCESS (SMSC accepted)
  16 = DLR_SMSC_FAIL    (SMSC rejected)

Usage:
  pip install redis aiomysql
  python3 test/dlr_writer.py
"""

import asyncio
import sys
import time

try:
    import redis.asyncio as aioredis
except ImportError:
    print("ERROR: pip install redis", file=sys.stderr)
    sys.exit(1)

try:
    import aiomysql
except ImportError:
    print("ERROR: pip install aiomysql", file=sys.stderr)
    sys.exit(1)

# ── config ────────────────────────────────────────────────────
REDIS_URL   = "redis://localhost"
REDIS_DB    = 2
STREAM_KEY  = "dlr_stream"

MYSQL_HOST  = "localhost"
MYSQL_PORT  = 3306
MYSQL_DB    = "sms"
MYSQL_USER  = "app"
MYSQL_PASS  = "<password>"

BATCH_SIZE  = 500
BLOCK_MS    = 1000   # block on XREAD for up to 1s

# ── SQL ───────────────────────────────────────────────────────
INSERT_SQL = (
    "INSERT INTO dlr_report "
    "(msg_id, foreign_id, status, smsc, src, dst, account, ts) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE "
    "status=VALUES(status), foreign_id=VALUES(foreign_id), "
    "updated_at=NOW()"
)


async def writer() -> None:
    redis = await aioredis.from_url(REDIS_URL, db=REDIS_DB,
                                    decode_responses=True)
    pool  = await aiomysql.create_pool(
        host=MYSQL_HOST, port=MYSQL_PORT,
        db=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS,
        minsize=2, maxsize=10, autocommit=False
    )

    last_id   = "0"
    buf       = []
    msg_ids   = []   # stream entry ids to ACK after commit
    written   = 0
    start_ts  = time.time()

    print(f"[dlr_writer] started — reading {STREAM_KEY} from Redis DB {REDIS_DB}",
          flush=True)

    while True:
        entries = await redis.xread(
            {STREAM_KEY: last_id}, count=BATCH_SIZE, block=BLOCK_MS
        )

        for _stream, msgs in (entries or []):
            for entry_id, fields in msgs:
                buf.append((
                    fields.get("id",     ""),   # msg_id (Kannel UUID)
                    fields.get("fid",    ""),   # foreign_id from SMSC
                    int(fields.get("status", 0)),
                    fields.get("smsc",   ""),
                    fields.get("from",   ""),
                    fields.get("to",     ""),
                    fields.get("acct",   ""),
                    int(fields.get("ts", 0)),
                ))
                msg_ids.append(entry_id)
                last_id = entry_id

        if not buf:
            continue

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(INSERT_SQL, buf)
                await conn.commit()

            # remove processed entries from stream
            await redis.xdel(STREAM_KEY, *msg_ids)

            written += len(buf)
            elapsed  = time.time() - start_ts
            tps      = written / elapsed if elapsed > 0 else 0
            print(f"[dlr_writer] written={written}  avg_tps={tps:.0f}/s",
                  flush=True)

        except Exception as exc:
            print(f"[dlr_writer] ERROR: {exc}", file=sys.stderr, flush=True)
            # leave entries in stream — will retry on next loop

        buf.clear()
        msg_ids.clear()


if __name__ == "__main__":
    asyncio.run(writer())
