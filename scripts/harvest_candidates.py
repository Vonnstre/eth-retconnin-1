"""Harvest recent addresses from block transactions within a lookback window.
Run as: python -m scripts.harvest_candidates --out data/raw/candidates.csv
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import List, Optional
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from .utils_eth import RpcPool, chunk_list

DEFAULT_LOOKBACK = int(os.getenv("HARVEST_LOOKBACK", "18000"))
DEFAULT_CONCURRENCY = 8
DEFAULT_BATCH = 120
PAUSE_BETWEEN_BATCHES = 0.05
BATCH_RPC_TIMEOUT = 60
SINGLE_RPC_TIMEOUT = 20
BATCH_RETRIES = 3
SINGLE_RETRIES = 3


async def _fetch_latest_block(pool: RpcPool) -> Optional[int]:
    for attempt in range(1, 6):
        ok, br = await pool.post({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}, timeout=12)
        if ok:
            if isinstance(br, dict) and "result" in br:
                try:
                    return int(br["result"], 16)
                except Exception:
                    pass
            if isinstance(br, str):
                try:
                    return int(br, 16)
                except Exception:
                    pass
        await asyncio.sleep(min(10, 0.5 * attempt))
    return None


async def _fetch_batch_blocks(pool: RpcPool, blocks: List[int]):
    payload = [
        {"jsonrpc": "2.0", "id": i, "method": "eth_getBlockByNumber", "params": [hex(n), True]}
        for i, n in enumerate(blocks)
    ]
    ok, resp = await pool.post(payload, timeout=BATCH_RPC_TIMEOUT)
    if not ok or not isinstance(resp, list):
        return None
    out = [None] * len(blocks)
    for item in resp:
        try:
            idx = int(item.get("id"))
            if 0 <= idx < len(out):
                out[idx] = item.get("result")
        except Exception:
            continue
    return out


async def _fetch_block_single(pool: RpcPool, n: int):
    ok, resp = await pool.post({"jsonrpc": "2.0", "id": n, "method": "eth_getBlockByNumber", "params": [hex(n), True]}, timeout=SINGLE_RPC_TIMEOUT)
    if not ok:
        return None
    if isinstance(resp, dict):
        return resp.get("result")
    return resp


async def _fetch_block_with_retries(pool: RpcPool, n: int):
    for attempt in range(1, SINGLE_RETRIES + 1):
        blk = await _fetch_block_single(pool, n)
        if blk is not None:
            return blk
        await asyncio.sleep(min(8, 0.4 * attempt))
    return None


async def main_async(out="data/raw/candidates.csv",
                     lookback_blocks=DEFAULT_LOOKBACK,
                     concurrency=DEFAULT_CONCURRENCY,
                     batch_size=DEFAULT_BATCH):
    Path(out).parent.mkdir(parents=True, exist_ok=True)

    pool = RpcPool(concurrency=concurrency)
    try:
        latest = await _fetch_latest_block(pool)
        if latest is None:
            print("ERROR: unable to determine latest block from RPC after retries. Writing empty candidates file and exiting.")
            Path(out).write_text("address\n")
            return

        start = max(1, latest - int(lookback_blocks))
        total_blocks = latest - start + 1
        print(f"Scanning blocks {start}..{latest} (~{lookback_blocks}) (batch={batch_size}, conc={concurrency})")

        addrs = set()
        processed = 0
        consecutive_batch_failures = 0

        for batch in chunk_list(list(range(start, latest + 1)), batch_size):
            batch_res = None
            for _ in range(BATCH_RETRIES):
                batch_res = await _fetch_batch_blocks(pool, batch)
                if batch_res is not None:
                    break
                await asyncio.sleep(0.8)

            if batch_res is None:
                consecutive_batch_failures += 1
                tasks = [_fetch_block_with_retries(pool, b) for b in batch]
                res = await asyncio.gather(*tasks, return_exceptions=True)
            else:
                consecutive_batch_failures = 0
                res = batch_res

            for blk in res:
                if isinstance(blk, Exception) or not blk:
                    continue
                for t in blk.get("transactions", []):
                    frm = t.get("from")
                    to = t.get("to")
                    if frm:
                        try:
                            from web3 import Web3
                            addrs.add(Web3.toChecksumAddress(frm))
                        except Exception:
                            addrs.add(frm)
                    if to:
                        try:
                            from web3 import Web3
                            addrs.add(Web3.toChecksumAddress(to))
                        except Exception:
                            addrs.add(to)

            processed += len(batch)
            print(f"... processed {processed}/{total_blocks} blocks, unique addresses: {len(addrs)} (last {batch[0]}..{batch[-1]})")

            if consecutive_batch_failures >= 3:
                await asyncio.sleep(5.0)
                consecutive_batch_failures = 0

            await asyncio.sleep(PAUSE_BETWEEN_BATCHES)

        pd.DataFrame({"address": sorted(addrs)}).to_csv(out, index=False)
        print(f"Wrote {out} rows: {len(addrs)}")

    finally:
        await pool.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw/candidates.csv")
    p.add_argument("--lookback-blocks", type=int, default=DEFAULT_LOOKBACK)
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    a = p.parse_args()
    try:
        asyncio.run(main_async(a.out, a.lookback_blocks, a.concurrency, a.batch_size))
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception as e:
        print("Harvest encountered error:", e)
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        if not Path(a.out).exists():
            Path(a.out).write_text("address\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
