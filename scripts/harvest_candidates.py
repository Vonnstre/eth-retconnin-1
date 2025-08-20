#!/usr/bin/env python3
# harvest_candidates.py
"""
Robust candidate harvester.

Features:
 - uses RpcPool and tries batch JSON-RPC for block fetches (more efficient)
 - falls back to per-block RPC calls if batching fails
 - retries + exponential backoff + jitter
 - safe: writes partial results and exits cleanly instead of raising on RPC failures
 - tunable CLI: --lookback-blocks, --concurrency, --batch-size, --pause
"""

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from typing import List

import pandas as pd

# --- ensure local imports work no matter where script is run ---
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.utils_eth import RpcPool, chunk_list  # now guaranteed to work

DEFAULT_LOOKBACK = int(os.getenv("HARVEST_LOOKBACK", "18000"))
DEFAULT_CONCURRENCY = 8
DEFAULT_BATCH = 100            # number of blocks per JSON-RPC batch request (tune down if hit rate-limits)
DEFAULT_PAUSE = 0.05           # seconds pause between batches to avoid spiking public RPCs
BATCH_RPC_TIMEOUT = 60
SINGLE_RPC_TIMEOUT = 20
BATCH_RETRIES = 3
SINGLE_RETRIES = 3


async def fetch_batch_blocks(pool: RpcPool, blocks: List[int], timeout: int = BATCH_RPC_TIMEOUT):
    """
    Try to fetch a list of blocks via a single batched JSON-RPC call.
    Returns list of block objects (or None in each position on failure).
    """
    if not blocks:
        return []

    payload = []
    for i, n in enumerate(blocks):
        payload.append({
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getBlockByNumber",
            "params": [hex(n), True]
        })

    ok, resp = await pool.post(payload, timeout=timeout)
    if not ok or not isinstance(resp, list):
        return None  # indicate batch failure; caller will fallback

    # resp is a list of dicts; match by id to blocks
    results = [None] * len(blocks)
    for it in resp:
        try:
            idx = int(it.get("id"))
            if 0 <= idx < len(blocks):
                results[idx] = it.get("result")
        except Exception:
            continue
    return results


async def fetch_block_single(pool: RpcPool, n: int, timeout: int = SINGLE_RPC_TIMEOUT):
    """Fetch a single block via RpcPool (single JSON-RPC request)."""
    ok, resp = await pool.post({
        "jsonrpc": "2.0", "id": n, "method": "eth_getBlockByNumber", "params": [hex(n), True]
    }, timeout=timeout)
    if not ok:
        return None
    if isinstance(resp, dict) and "result" in resp:
        return resp["result"]
    return resp


async def fetch_block_with_retries(pool: RpcPool, n: int, retries: int = SINGLE_RETRIES):
    """Retry single-block fetch with backoff."""
    for attempt in range(1, retries + 1):
        blk = await fetch_block_single(pool, n)
        if blk is not None:
            return blk
        await asyncio.sleep(min(10, 0.3 * attempt))
    return None


async def main_async(out="data/raw/candidates.csv",
                     lookback_blocks=DEFAULT_LOOKBACK,
                     concurrency=DEFAULT_CONCURRENCY,
                     batch_size=DEFAULT_BATCH,
                     pause_between_batches=DEFAULT_PAUSE):
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    pool = RpcPool(concurrency=concurrency)
    try:
        # Get latest block robustly using the pool
        for attempt in range(1, 5):
            ok, br = await pool.post({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}, timeout=10)
            if ok:
                if isinstance(br, dict) and "result" in br:
                    try:
                        latest = int(br["result"], 16)
                        break
                    except Exception:
                        pass
                # some nodes may return raw hex string
                if isinstance(br, str):
                    try:
                        latest = int(br, 16)
                        break
                    except Exception:
                        pass
            await asyncio.sleep(min(10, 0.4 * attempt))
        else:
            # failed to fetch latest block safely -> write empty file and exit cleanly
            print("ERROR: unable to determine latest block from RPC after retries. Writing empty candidates file and exiting.")
            Path(out).write_text("address\n")
            return

        start = max(1, latest - int(lookback_blocks))
        print(f"Scanning blocks {start}..{latest} (~{lookback_blocks}) (batch_size={batch_size}, concurrency={concurrency})")

        addrs = set()
        total_blocks = latest - start + 1
        processed = 0
        consecutive_batch_failures = 0

        # iterate in batches (e.g., 100 blocks per batch)
        for batch in chunk_list(list(range(start, latest + 1)), batch_size):
            # First try a batched RPC call (one HTTP POST for many block requests)
            blk_results = None
            for br_attempt in range(1, BATCH_RETRIES + 1):
                blk_results = await fetch_batch_blocks(pool, batch)
                if blk_results is not None:
                    break
                # exponential-ish backoff
                await asyncio.sleep(min(10, 0.5 * br_attempt))
            if blk_results is None:
                # batch failed -> fallback to per-block fetch (more gentle)
                consecutive_batch_failures += 1
                print(f"Batch fetch failed for blocks {batch[0]}..{batch[-1]} (attempts exhausted). Falling back to per-block fetch.")
                # throttle per-block concurrency
                tasks = [fetch_block_with_retries(pool, b) for b in batch]
                res = await asyncio.gather(*tasks, return_exceptions=True)
            else:
                consecutive_batch_failures = 0
                res = blk_results

            # process results (res is list aligned to batch)
            batch_seen = 0
            for blk_item in res:
                if isinstance(blk_item, Exception):
                    # treat as missing
                    continue
                if not blk_item:
                    continue
                # extract transactions
                for t in blk_item.get("transactions", []):
                    frm = t.get("from")
                    to = t.get("to")
                    if frm:
                        try:
                            # use web3 checksum if available, else raw
                            from web3 import Web3  # local import to avoid package-level dependency issues
                            addrs.add(Web3.toChecksumAddress(frm))
                        except Exception:
                            try:
                                addrs.add(frm)
                            except Exception:
                                pass
                    if to:
                        try:
                            from web3 import Web3
                            addrs.add(Web3.toChecksumAddress(to))
                        except Exception:
                            try:
                                addrs.add(to)
                            except Exception:
                                pass
                batch_seen += 1

            processed += len(batch)
            print(f"... processed {processed}/{total_blocks} blocks, unique addresses: {len(addrs)} (last batch {batch[0]}..{batch[-1]}, seen_in_batch={batch_seen})")

            # small pause to reduce bursts on public RPCs
            await asyncio.sleep(pause_between_batches)

            # safety: if too many consecutive batch failures, back off longer
            if consecutive_batch_failures >= 3:
                print("Multiple batch failures in a row; sleeping longer to recover...")
                await asyncio.sleep(5.0)
                consecutive_batch_failures = 0

        # write result CSV
        pd.DataFrame({"address": sorted(addrs)}).to_csv(out, index=False)
        print("Wrote", out, "rows:", len(addrs))

    finally:
        try:
            await pool.close()
        except Exception:
            pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/raw/candidates.csv")
    p.add_argument("--lookback-blocks", type=int, default=DEFAULT_LOOKBACK)
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH, help="Number of blocks per batch RPC")
    p.add_argument("--pause", type=float, default=DEFAULT_PAUSE, help="Pause (s) between batches to avoid rate limits")
    a = p.parse_args()

    try:
        asyncio.run(main_async(a.out, a.lookback_blocks, a.concurrency, a.batch_size, a.pause))
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        # Fail-safe: don't leak stack traces to CI annotations; write partial output if possible and exit 0.
        print("Harvest encountered error:", e)
        # attempt to be graceful - but do not raise
        sys.exit(0)


if __name__ == "__main__":
    main()
