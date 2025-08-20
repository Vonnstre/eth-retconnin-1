#!/usr/bin/env python3
import argparse
import math
from pathlib import Path
import pandas as pd

def split(input_path: Path, parts: int, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(input_path) if input_path.exists() else df = pd.DataFrame({"address":[]})
    n = len(df)
    rows = math.ceil(n / parts) if n > 0 else 0
    for i in range(parts):
        s = df.iloc[i*rows:(i+1)*rows] if rows > 0 else df.iloc[0:0]
        s.to_csv(outdir / f"shard_{i+1:02d}.csv", index=False)
    print("split into", parts, "shards")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--parts", type=int, default=20)
    p.add_argument("--outdir", required=True)
    a = p.parse_args()
    split(Path(a.input), a.parts, Path(a.outdir))
