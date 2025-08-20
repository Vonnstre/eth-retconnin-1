# harvest_candidates.py
import argparse
import asyncio
import os
from pathlib import Path
from web3 import Web3
import pandas as pd
from scripts.utils_eth import RpcPool, chunk_list

DEFAULT_LOOKBACK = int(os.getenv("HARVEST_LOOKBACK", "18000"))

async def get_block(pool: RpcPool, n: int):
    ok, resp = await pool.post({"jsonrpc": "2.0", "id": n, "method": "eth_getBlockByNumber", "params": [hex(n), True]}, timeout=30)
    if not ok:
        return None
    if isinstance(resp, dict) and 'result' in resp:
        return resp['result']
    return resp

async def main_async(out="data/raw/candidates.csv", lookback_blocks=DEFAULT_LOOKBACK, concurrency=12):
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    pool = RpcPool(concurrency=concurrency)
    try:
        # get latest block via RPC pool for deterministic snapshot
        ok, latest_resp = await pool.post({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}, timeout=10)
        if ok and isinstance(latest_resp, dict) and 'result' in latest_resp:
            latest = int(latest_resp['result'], 16)
        else:
            # fallback to local web3 simple provider
            w3 = Web3(Web3.HTTPProvider(pool._pick()))
            latest = w3.eth.block_number
        start = max(1, latest - lookback_blocks)
        print(f"Scanning blocks {start}..{latest} (~{lookback_blocks})")
        addrs = set()
        for batch in chunk_list(list(range(start, latest + 1)), 200):
            tasks = [get_block(pool, b) for b in batch]
            res = await asyncio.gather(*tasks)
            for blk in res:
                if not blk: continue
                for t in blk.get("transactions", []):
                    frm = t.get("from"); to = t.get("to")
                    if frm:
                        try: addrs.add(Web3.toChecksumAddress(frm))
                        except Exception: pass
                    if to:
                        try: addrs.add(Web3.toChecksumAddress(to))
                        except Exception: pass
            print(f"... seen unique addresses: {len(addrs)} (up to block {batch[-1]})")
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
    p.add_argument("--concurrency", type=int, default=12)
    a = p.parse_args()
    asyncio.run(main_async(a.out, a.lookback_blocks, a.concurrency))

if __name__ == "__main__":
    main()
