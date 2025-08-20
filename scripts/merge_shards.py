#!/usr/bin/env python3
import argparse
import glob
import os
from pathlib import Path
import pandas as pd

def merge(indir: Path, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    # prefer final per-shard top file if present
    paths = sorted(glob.glob(str(indir) + "/**/top10k_whales_raw.csv", recursive=True))
    dfs = []
    for p in paths:
        try:
            dfs.append(pd.read_csv(p))
        except Exception:
            pass
    if not dfs:
        # fallback to any balances_with_usd or per-shard merged outputs
        paths = sorted(glob.glob(str(indir) + "/**/balances_with_usd.csv", recursive=True))
        for p in paths:
            try:
                dfs.append(pd.read_csv(p))
            except Exception:
                pass
    if not dfs:
        raise SystemExit("No shard outputs found in artifacts")
    full = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['address']).reset_index(drop=True)
    outdir_join = Path(outdir)
    (outdir_join / "merged.csv").write_text(full.to_csv(index=False))
    if 'usd_value' in full.columns:
        top = full.sort_values('usd_value', ascending=False).head(10000)
    else:
        top = full.head(10000)
    (outdir_join / "top10k_whales_raw.csv").write_text(top.to_csv(index=False))
    print("Final top10k rows:", len(top))

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--indir", required=True)
    p.add_argument("--outdir", required=True)
    a = p.parse_args()
    merge(Path(a.indir), Path(a.outdir))
