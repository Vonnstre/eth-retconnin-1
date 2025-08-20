# token_multicall_simple.py
import asyncio
from pathlib import Path
from scripts.utils_eth import RpcPool, chunk_list

ERC20_BALANCE_OF_SIG = '0x70a08231'  # balanceOf(address)

async def fetch_token_balances(addresses, token_meta, out_path, concurrency=14, batch=200):
    pool = RpcPool(concurrency=concurrency)
    results = {a: {} for a in addresses}

    async def call_balance(token_addr, a):
        # normalize address hex (32-byte right-padded address in calldata)
        addr_hex = a[2:] if a.startswith('0x') else a
        addr_hex = addr_hex.lower().zfill(64)
        data = ERC20_BALANCE_OF_SIG + addr_hex
        ok, resp = await pool.post({"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": token_addr, "data": data}, "latest"]}, timeout=20)
        if not ok:
            return 0
        try:
            if isinstance(resp, dict) and 'result' in resp:
                val = resp.get('result')
            else:
                val = resp
            if isinstance(val, dict) and 'result' in val:
                val = val['result']
            return int(val or "0x0", 16)
        except Exception:
            return 0

    for sym, meta in token_meta.items():
        token_addr = meta.get("address")
        if not token_addr:
            continue
        for b in chunk_list(addresses, batch):
            tasks = [call_balance(token_addr, a) for a in b]
            vals = await asyncio.gather(*tasks)
            for a, v in zip(b, vals):
                results[a][f"{sym}_raw"] = v

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    syms = list(token_meta.keys())
    with open(out_path, "w") as f:
        f.write("address," + ",".join([s + "_raw" for s in syms]) + "\n")
        for a in addresses:
            row = [str(results[a].get(s + "_raw", 0)) for s in syms]
            f.write(a + "," + ",".join(row) + "\n")
    await pool.close()
    return out_path
