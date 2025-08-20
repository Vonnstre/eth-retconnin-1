"""Main shard pipeline (CI-friendly)
Run as module from repo root:
python -m scripts.bulletproof_pipeline --input data/raw/candidates.csv --out data/run_shard
"""
import argparse
import asyncio
import json
import os
import time
import zipfile
import shutil
from pathlib import Path
import pandas as pd
from web3 import Web3

from .utils_eth import RpcPool, chunk_list
from .token_multicall_simple import fetch_token_balances
from .lead_score import compute_lead_score
from .exchange_detect import detect_exchange_inflow, load_exchange_set
from .merkle_proofs import compute_merkle

# env/config
TOKENS_JSON = os.getenv("TOKENS_JSON", "") or ""
ETH_PRICE_SOURCE = os.getenv("ETH_PRICE_SOURCE", "coingecko")
ETH_PRICE_USD_MANUAL = float(os.getenv("ETH_PRICE_USD", "3000"))

DEFAULT_TOKENS = {
    "USDC": {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6, "usd": 1},
    "USDT": {"address": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6, "usd": 1},
    "WETH": {"address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "decimals": 18, "peg": "ETH"},
    "WBTC": {"address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "decimals": 8, "coingecko_id": "wrapped-bitcoin"},
}


def load_tokens():
    try:
        extra = json.loads(TOKENS_JSON) if TOKENS_JSON else {}
    except Exception:
        extra = {}
    t = DEFAULT_TOKENS.copy()
    t.update(extra)
    return t


def fetch_prices_coingecko(ids):
    if not ids:
        return {}
    try:
        import requests
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": ",".join(ids), "vs_currencies": "usd"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


def get_market_prices(token_meta):
    eth = ETH_PRICE_USD_MANUAL
    btc = None
    if ETH_PRICE_SOURCE == "coingecko":
        ids = []
        for sym, m in token_meta.items():
            if m.get("coingecko_id"):
                ids.append(m.get("coingecko_id"))
        ids = sorted(set(ids + ["ethereum"]))
        prices = fetch_prices_coingecko(ids)
        eth = float(prices.get("ethereum", {}).get("usd", eth) or eth)
        if token_meta.get("WBTC", {}).get("coingecko_id"):
            wbtc_id = token_meta["WBTC"]["coingecko_id"]
            btc = float(prices.get(wbtc_id, {}).get("usd", 0) or 0)
    if not btc:
        btc = eth * 14.0
    return {"eth": eth, "btc": btc}


def decimals_div(v, dec):
    try:
        return float(v) / (10 ** dec)
    except Exception:
        return 0.0


def compute_usd(df, token_meta, eth_usd, btc_usd):
    df = df.copy()
    df["usd_eth_only"] = df["eth_balance"].astype(float) * eth_usd
    for sym, meta in token_meta.items():
        raw = f"{sym}_raw"
        col = f"token_{sym}_usd"
        if raw not in df.columns:
            df[raw] = 0
        if sym in ["USDC", "USDT"]:
            df[col] = df[raw].fillna(0).astype(float).apply(lambda x: decimals_div(x, meta["decimals"]) * 1.0)
        elif sym == "WETH":
            df[col] = df[raw].fillna(0).astype(float).apply(lambda x: decimals_div(x, meta["decimals"]) * eth_usd)
        elif sym == "WBTC":
            df[col] = df[raw].fillna(0).astype(float).apply(lambda x: decimals_div(x, meta["decimals"]) * btc_usd)
        else:
            df[col] = 0.0
    token_usd_cols = [c for c in df.columns if c.startswith("token_") and c.endswith("_usd")]
    df["token_portfolio_usd"] = df[token_usd_cols].sum(axis=1) if token_usd_cols else 0.0
    df["stablecoin_usd"] = df.get("token_USDC_usd", 0) + df.get("token_USDT_usd", 0)
    df["usd_value"] = (df["usd_eth_only"] + df["token_portfolio_usd"]).round(2)
    df["stablecoin_pct"] = (df["stablecoin_usd"] / df["usd_value"]).replace([float("inf")], 0).fillna(0).clip(0, 1)
    return df


async def batch_eth_getBalance(pool: RpcPool, addrs, batch=150):
    balances = {}
    if not addrs:
        return balances
    for chunk in chunk_list(addrs, batch):
        payload = [
            {"jsonrpc": "2.0", "id": i, "method": "eth_getBalance", "params": [a, "latest"]}
            for i, a in enumerate(chunk)
        ]
        ok, resp = await pool.post(payload, timeout=60)
        if not ok:
            for a in chunk:
                balances[a] = 0.0
            continue
        items = resp if isinstance(resp, list) else []
        for it in items:
            try:
                idx = int(it.get("id"))
                res = it.get("result")
                addr = chunk[idx]
                v = int(res, 16) if res else 0
                balances[addr] = float(Web3.fromWei(v, "ether")) if v else 0.0
            except Exception:
                pass
    return balances


def read_candidates(path):
    if not Path(path).exists():
        return []
    df = pd.read_csv(path, dtype=str)
    col = "address" if "address" in df.columns else df.columns[0]
    addrs = df[col].astype(str).tolist()
    out = []
    for a in addrs:
        try:
            out.append(Web3.toChecksumAddress(a))
        except Exception:
            pass
    return list(dict.fromkeys(out))


def run_pipeline(
    input_csv,
    out_dir,
    usd_threshold=10000,
    top_n_multicall=3000,
    eth_conc=12,
    eth_batch=120,
    token_conc=14,
    token_batch=500,
):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if not Path(input_csv).exists():
        print("No candidate file found; aborting shard run")
        return out

    addrs = read_candidates(input_csv)
    print("Candidates in shard:", len(addrs))

    pool = RpcPool(concurrency=eth_conc, urls=os.getenv("ETH_RPC_URL"))
    try:
        balances = asyncio.run(batch_eth_getBalance(pool, addrs, batch=eth_batch))
    finally:
        try:
            asyncio.run(pool.close())
        except Exception:
            pass

    df = pd.DataFrame({"address": addrs})
    df["eth_balance"] = df["address"].map(balances).fillna(0).astype(float)

    try:
        w3 = Web3(Web3.HTTPProvider(os.getenv("ETH_RPC_URL", "https://cloudflare-eth.com")))
        df["snapshot_block"] = w3.eth.block_number
    except Exception:
        df["snapshot_block"] = 0
    df["snapshot_ts_utc"] = int(time.time())

    token_meta = load_tokens()
    prices = get_market_prices(token_meta)
    eth_usd = prices["eth"]
    btc_usd = prices["btc"]

    df = compute_usd(df, token_meta, eth_usd, btc_usd)

    # enrich top N for token balances
    enrich_targets = df.sort_values("usd_value", ascending=False).head(min(top_n_multicall, len(df)))
    enrich_targets = enrich_targets["address"].tolist()
    token_tmp = out / "tmp_token_balances.csv"
    token_tmp.parent.mkdir(parents=True, exist_ok=True)

    asyncio.run(fetch_token_balances(enrich_targets, token_meta, str(token_tmp), concurrency=token_conc, batch=token_batch))

    if token_tmp.exists():
        tdf = pd.read_csv(token_tmp, dtype=str)
        for c in tdf.columns:
            if c != "address":
                tdf[c] = pd.to_numeric(tdf[c], errors="coerce").fillna(0)
        df = df.merge(tdf, on="address", how="left").fillna(0)
        df = compute_usd(df, token_meta, eth_usd, btc_usd)

    df = detect_exchange_inflow(df, load_exchange_set())
    df = compute_lead_score(df)

    whales = df[df["usd_value"] >= usd_threshold].sort_values("usd_value", ascending=False).reset_index(drop=True)
    top_final = whales.head(10000)
    (out / "top10k_whales_raw.csv").write_text(top_final.to_csv(index=False))
    print("Top rows in shard:", len(top_final))

    delivery_dir = out / "delivery"
    delivery_dir.mkdir(parents=True, exist_ok=True)

    compute_merkle(Path(out / "top10k_whales_raw.csv"), delivery_dir, sample_indices=list(range(0, min(20, len(top_final)))))

    # Call proof_and_sample as module-run (safe)
    os.system(f"python -m scripts.proof_and_sample --dataset {out/'top10k_whales_raw.csv'} --out {delivery_dir} --sample-size 100")

    try:
        shutil.copy("scripts/buyer_verifier.sh", delivery_dir / "buyer_verifier.sh")
    except Exception:
        pass
    if Path("docs/DELIVERY_README.md").exists():
        (delivery_dir / "docs").mkdir(exist_ok=True)
        shutil.copy("docs/DELIVERY_README.md", delivery_dir / "docs/DELIVERY_README.md")

    zname = delivery_dir / f"Top10k_Delivery_{int(time.time())}.zip"
    with zipfile.ZipFile(zname, "w", zipfile.ZIP_DEFLATED) as z:
        for p in delivery_dir.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(delivery_dir))
    print("Wrote", zname)
    return delivery_dir


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw/candidates.csv")
    ap.add_argument("--out", default="data/run_shard")
    ap.add_argument("--usd-threshold", type=int, default=10000)
    ap.add_argument("--top-n-multicall", type=int, default=3000)
    ap.add_argument("--eth-conc", type=int, default=12)
    ap.add_argument("--eth-batch", type=int, default=120)
    ap.add_argument("--token-conc", type=int, default=14)
    ap.add_argument("--token-batch", type=int, default=500)
    a = ap.parse_args()
    run_pipeline(a.input, a.out, a.usd_threshold, a.top_n_multicall, a.eth_conc, a.eth_batch, a.token_conc, a.token_batch)
