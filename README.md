# BlackOps LeadGen — Institutional Edition (One-off)

**One-run product:** produce `Top10kWhales_USD>=10k_Enriched_<run_id>.zip` — 10k enriched Ethereum addresses (USD >= $10k) with cryptographic proofs, token USD valuations, recent-activity signals, explainable lead score, exchange flags, cluster tags, dashboard, and buyer verifier.

Key points:
- Zero paid API keys required (defaults use public RPCs + CoinGecko).
- Workflow is tuned to finish under 6 hours on GitHub Actions default runners (sharded).
- The delivered ZIP contains only buyer-facing files (public key, manifest, merkle root, proofs); private signing key is removed.

Usage (producer):
1. Create a **private** GitHub repo and paste these files in exact layout.
2. Commit & push.
3. In GitHub Actions run the "Build & Publish (Top-10k)" workflow.
4. Download the artifact `top10k_delivery` (ZIP) when workflow finishes.

Buyer verification quick:
