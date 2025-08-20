#!/usr/bin/env bash
set -e
cmd=$1

if [ "$cmd" == "verify-manifest" ]; then
    manifest=$2; sig=$3; pub=$4
    python - <<PY
from pathlib import Path
from cryptography.hazmat.primitives import serialization
m = Path("$manifest").read_bytes()
sig = Path("$sig").read_bytes()
pub = Path("$pub").read_bytes()
pk = serialization.load_pem_public_key(pub)
try:
    pk.verify(sig, m)
    print("Manifest signature: VALID")
except Exception as e:
    print("Manifest signature: INVALID", e)
    raise SystemExit(2)
PY
    exit 0
fi

if [ "$cmd" == "verify-merkle" ]; then
    mr=$2; sig=$3; pub=$4
    python - <<PY
from pathlib import Path
from cryptography.hazmat.primitives import serialization
m = Path("$mr").read_text().strip().encode()
sig = Path("$sig").read_bytes()
pub = Path("$pub").read_bytes()
pk = serialization.load_pem_public_key(pub)
try:
    pk.verify(sig, m)
    print("Merkle root signature: VALID")
except Exception as e:
    print("Merkle root signature: INVALID", e)
    raise SystemExit(2)
PY
    exit 0
fi

if [ "$cmd" == "verify-proof" ]; then
    proof=$2; mr=$3
    python - <<PY
import json, hashlib
from pathlib import Path
p = json.load(open("$proof"))
row = p['row']
root_expected = Path("$mr").read_text().strip()
leaf_bytes = ('\x1f'.join([str(x) for x in row])).encode()
leaf = hashlib.sha256(b'\x00' + leaf_bytes).digest()
cur = leaf
for item in p['proof']:
    sib = bytes.fromhex(item['hash'])
    if item.get('is_left'):
        cur = hashlib.sha256(b'\x01' + sib + cur).digest()
    else:
        cur = hashlib.sha256(b'\x01' + cur + sib).digest()
print('computed_root:', cur.hex())
print('expected_root:', root_expected)
print('match:', cur.hex() == root_expected)
PY
    exit 0
fi

echo "Usage:"
echo "  scripts/buyer_verifier.sh verify-manifest manifest.json manifest.json.sig ed25519_public.pem"
echo "  scripts/buyer_verifier.sh verify-merkle merkle_root.txt merkle_root.sig ed25519_public.pem"
echo "  scripts/buyer_verifier.sh verify-proof inclusion_proofs/sample_row_0.json merkle_root.txt"
exit 1
