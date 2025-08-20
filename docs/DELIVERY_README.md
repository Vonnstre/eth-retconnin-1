# Delivery README

This folder contains the delivery artifacts for the Top-10k whales dataset.

Files included in the buyer zip:
- **top10k_whales_raw.csv** — CSV of the final top addresses and enriched columns
- **merkle_root.txt** — hex merkle root of the CSV
- **inclusion_proofs/** — JSON files with sample inclusion proofs (row + proof)
- **manifest.json** and **manifest.json.sig** — dataset manifest and signature
- **ed25519_public.pem** — public key for verifying signatures
- **merkle_root.sig** — signature over the merkle root (optional, if signing key provided)
- **dashboard/** — a small HTML preview of the dataset
- **scripts/buyer_verifier.sh** — utility script buyers can use to verify manifest/merkle/proofs

Verification steps for buyers:
1. Verify manifest signature:
   `scripts/buyer_verifier.sh verify-manifest manifest.json manifest.json.sig ed25519_public.pem`
2. Verify merkle root signature (if provided):
   `scripts/buyer_verifier.sh verify-merkle merkle_root.txt merkle_root.sig ed25519_public.pem`
3. Verify an inclusion proof:
   `scripts/buyer_verifier.sh verify-proof inclusion_proofs/sample_row_0.json merkle_root.txt`

Notes for operators:
- Do **NOT** include private keys in the delivery zip. Keep signing keys in a secure location or CI secret.
- If using free RPC endpoints, shard the workload (the provided workflow uses 10 shards by default).
- You can extend exchange lists by adding lines to `data/config/exchanges.csv` (one address per line).
