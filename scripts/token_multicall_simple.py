"""Reliable token balance fetch using batched eth_call (no ABI encoding dependency).
This sacrifices multicall efficiency for simplicity and robustness with public RPCs.
"""
import asyncio
from pathlib import Path
from typing import List
from .utils_eth import RpcPool, chunk_list

ERC20_BALANCE_OF_SIG = "70a08231"  # no 0x

async def fetch_token_balances(
    addresses: List[str],
    token_meta: dict,
    out_path: str,
    concurrency: int = 14,
    batch: int = 200,
) -> str:
    pool = RpcPool(concurrency=concurrency)
    results = {a: {} for a in addresses}
    syms = list(token_meta.keys())

    # For each token, send batch eth_call requests grouped for speed
    for sym in syms:
        meta = token_meta.get(sym, {})
        token_addr = meta.get("address")
        if not token_addr:
            # ensure zeros later
            for a in addresses:
                results[a][f"{sym}_raw"] = 0
            continue
        token_addr = token_addr.lower().replace("0x", "")

        for grp in chunk_list(addresses, batch):
            payload = []
            for i, a in enumerate(grp):
                addr_hex = a.lower().replace("0x", "").rjust(64, "0")
                data = "0x" + ERC20_BALANCE_OF_SIG + addr_hex
                # each entry needs unique id so we can map back
                payload.append({
                    "jsonrpc": "2.0",
                    "id": i,
                    "method": "eth_call",
                    "params": [{"to": token_addr if token_addr.startswith("0x") else "0x" + token_addr, "data": data}, "latest"],
                })
            ok, resp = await pool.post(payload, timeout=60)
            if not ok or not isinstance(resp, list):
                for a in grp:
                    results[a][f"{sym}_raw"] = 0
                continue
            # resp is list aligned with grp by id
            for item in resp:
                try:
                    idx = int(item.get("id"))
                    res = item.get("result")
                    addr = grp[idx]
                    if not res:
                        val = 0
                    else:
                        h = res[2:] if res.startswith("0x") else res
                        val = int(h, 16) if h else 0
                except Exception:
                    val = 0
                results[addr][f"{sym}_raw"] = val

    # write CSV
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        f.write("address," + ",".join([s + "_raw" for s in syms]) + "\n")
        for a in addresses:
            row = [str(results[a].get(s + "_raw", 0)) for s in syms]
            f.write(a + "," + ",".join(row) + "\n")

    await pool.close()
    return out_path
