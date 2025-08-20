# verify_row.py
import json
import hashlib
import sys
from pathlib import Path

def canonical_row_bytes(row):
    return ('\x1f'.join([str(c) if c is not None else '' for c in row])).encode('utf-8')

def Hleaf(b: bytes) -> bytes:
    return hashlib.sha256(b'\x00' + b).digest()

def Hnode(a: bytes, b: bytes) -> bytes:
    return hashlib.sha256(b'\x01' + a + b).digest()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/verify_row.py <proof_json> <merkle_root.txt>")
        sys.exit(2)
    proof = json.loads(Path(sys.argv[1]).read_text())
    root = Path(sys.argv[2]).read_text().strip()
    cur = Hleaf(canonical_row_bytes(proof['row']))
    for p in proof['proof']:
        sib = bytes.fromhex(p['hash'])
        if p.get('is_left'):
            cur = Hnode(sib, cur)
        else:
            cur = Hnode(cur, sib)
    print("computed_root:", cur.hex())
    print("expected_root:", root)
    print("match:", cur.hex() == root)
