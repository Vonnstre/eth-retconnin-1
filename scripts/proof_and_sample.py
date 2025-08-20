import argparse
import json
import time
import hashlib
from pathlib import Path
import pandas as pd
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--dataset', required=True)
    ap.add_argument('--out', default='data/run_final/delivery')
    ap.add_argument('--sample-size', type=int, default=100)
    ap.add_argument('--privkey', default='')
    a = ap.parse_args()

    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(a.dataset)

    top = df.sort_values('usd_value', ascending=False).head(min(50, len(df)))
    rem = df.drop(top.index, errors='ignore')
    n_rand = max(0, a.sample_size - len(top))
    rand = rem.sample(n=min(n_rand, len(rem))) if len(rem) > 0 and n_rand > 0 else df.iloc[0:0]
    sample = pd.concat([top, rand]).drop_duplicates().head(a.sample_size).reset_index(drop=True)
    spath = out / 'sample_preview.csv'; sample.to_csv(spath, index=False)

    manifest = {
        "dataset_file": Path(a.dataset).name,
        "dataset_sha256": sha256_file(Path(a.dataset)),
        "sample_file": spath.name,
        "sample_sha256": sha256_file(spath),
        "merkle_root_file": "merkle_root.txt",
        "timestamp_utc": int(time.time())
    }
    mpath = out / 'manifest.json'; mpath.write_text(json.dumps(manifest, indent=2))

    if a.privkey:
        priv_bytes = Path(a.privkey).read_bytes()
        sk = serialization.load_pem_private_key(priv_bytes, password=None)
    else:
        sk = Ed25519PrivateKey.generate()

    pk = sk.public_key()
    (out / 'ed25519_public.pem').write_bytes(
        pk.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )

    sig = sk.sign(mpath.read_bytes())
    (mpath.with_suffix('.json.sig')).write_bytes(sig)

    print("Wrote manifest and signature to", out)
