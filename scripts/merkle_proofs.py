from pathlib import Path
import hashlib
import json
import argparse
import csv


def canonical_row_bytes(row):
    return ('\x1f'.join([str(c) if c is not None else '' for c in row])).encode('utf-8')


def Hleaf(b: bytes) -> bytes:
    return hashlib.sha256(b'\x00' + b).digest()


def Hnode(a: bytes, b: bytes) -> bytes:
    return hashlib.sha256(b'\x01' + a + b).digest()


class MerkleTree:
    def __init__(self, leaves):
        self.leaves = [Hleaf(l) for l in leaves]
        if not self.leaves:
            self.layers = []
        else:
            self.layers = [self.leaves]
            while len(self.layers[-1]) > 1:
                layer = self.layers[-1]
                nxt = []
                for i in range(0, len(layer), 2):
                    a = layer[i]
                    b = layer[i + 1] if i + 1 < len(layer) else layer[i]
                    nxt.append(Hnode(a, b))
                self.layers.append(nxt)

    def root(self):
        if not self.layers:
            return b''
        return self.layers[-1][0]

    def proof(self, idx):
        proof = []
        cur = idx
        for layer in self.layers[:-1]:
            pair = cur ^ 1
            if pair < len(layer):
                sib = layer[pair]
                is_left = (pair % 2 == 0)
                proof.append({"hash": sib.hex(), "is_left": is_left})
            else:
                proof.append({"hash": layer[cur].hex(), "is_left": False})
            cur //= 2
        return proof


def compute_merkle(csv_path: Path, out_dir: Path, sample_indices):
    rows = []
    with csv_path.open('r', newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        for r in reader:
            rows.append(r)
    leaves = [canonical_row_bytes(r) for r in rows]
    mt = MerkleTree(leaves)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / 'merkle_root.txt').write_text(mt.root().hex())

    proofs_dir = out_dir / 'inclusion_proofs'
    proofs_dir.mkdir(exist_ok=True)
    for idx in sample_indices:
        if 0 <= idx < len(rows):
            p = {
                "index": idx,
                "row": rows[idx],
                "proof": mt.proof(idx)
            }
            (proofs_dir / f'sample_row_{idx}.json').write_text(json.dumps(p, indent=2))
    return mt.root().hex()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--out', default='data/run_final/delivery')
    ap.add_argument('--samples', nargs='*', type=int, default=[0,1,2,3,4,5,6,7,8,9])
    a = ap.parse_args()
    root = compute_merkle(Path(a.input), Path(a.out), a.samples)
    print("Merkle root:", root)
