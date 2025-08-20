# token_multicall_simple.py
import asyncio
from pathlib import Path
from typing import List
from scripts.utils_eth import RpcPool, chunk_list, DEFAULT_RPC
from web3 import Web3

# ERC20 balanceOf selector
ERC20_BALANCE_OF_SIG = "0x70a08231"

# Multicall2 contract (mainnet). If you target another network, change this.
MULTICALL2_ADDRESS = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696"

# Minimal ABI for aggregate((address,bytes)[])
MULTICALL2_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "target", "type": "address"},
                    {"internalType": "bytes", "name": "callData", "type": "bytes"},
                ],
                "internalType": "struct Multicall.Call[]",
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate",
        "outputs": [
            {"internalType": "uint256", "name": "blockNumber", "type": "uint256"},
            {"internalType": "bytes[]", "name": "returnData", "type": "bytes[]"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


async def fetch_token_balances(
    addresses: List[str],
    token_meta: dict,
    out_path: str,
    concurrency: int = 14,
    batch: int = 200,
) -> str:
    """
    Uses a Multicall aggregate() to fetch many ERC20 balanceOf() results in batches.
    - addresses: list of checksum addresses (strings)
    - token_meta: dict of token symbol -> {"address": "...", "decimals": n}
    - out_path: file path to write CSV: address,<SYM>_raw,...
    - concurrency: number of concurrent multicall requests
    - batch: number of addresses per multicall batch (per token)
    Returns out_path on success.
    """
    pool = RpcPool(concurrency=concurrency)
    results = {a: {} for a in addresses}
    syms = list(token_meta.keys())

    # Setup a w3 purely for ABI encoding/decoding (no network calls here)
    w3 = Web3(Web3.HTTPProvider(DEFAULT_RPC))

    # Prepare contract object for encoding/decoding
    multicall_contract = w3.eth.contract(address=Web3.toChecksumAddress(MULTICALL2_ADDRESS), abi=MULTICALL2_ABI)

    async def _rpc_multicall(calls):
        """calls is list of (target_address, calldata_bytes) tuples"""
        try:
            data = multicall_contract.encodeABI(fn_name="aggregate", args=[calls])
        except Exception as e:
            return False, {"error": f"encodeABI_failed: {e}"}
        ok, resp = await pool.post(
            {"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": MULTICALL2_ADDRESS, "data": data}, "latest"]},
            timeout=60,
        )
        if not ok:
            return False, resp
        # resp may be dict with 'result' hex string
        if isinstance(resp, dict) and "result" in resp:
            raw = resp["result"]
        else:
            raw = resp if isinstance(resp, str) else None
            if isinstance(resp, dict) and "error" in resp:
                return False, resp
        if not raw:
            return False, {"error": "empty_result"}
        try:
            # decode aggregate return: (uint256, bytes[])
            b = bytes.fromhex(raw[2:] if raw.startswith("0x") else raw)
            decoded = w3.codec.decode_abi(["uint256", "bytes[]"], b)
            return True, decoded  # (blockNumber, [bytes, bytes, ...])
        except Exception as e:
            return False, {"error": f"decode_failed: {e}", "raw": raw[:200]}

    # For each token, iterate addresses in batches and call multicall
    for sym, meta in token_meta.items():
        token_addr = meta.get("address")
        if not token_addr:
            # write zeros later; skip
            continue
        token_addr = Web3.toChecksumAddress(token_addr)
        for group in chunk_list(addresses, batch):
            calls = []
            for a in group:
                # prepare balanceOf calldata: selector + 32-byte left-padded address
                addr_hex = a[2:] if a.startswith("0x") else a
                addr_hex = addr_hex.zfill(64).lower()
                calldata_hex = ERC20_BALANCE_OF_SIG + addr_hex
                calldata_bytes = bytes.fromhex(calldata_hex[2:] if calldata_hex.startswith("0x") else calldata_hex)
                calls.append((token_addr, calldata_bytes))
            ok, decoded = await _rpc_multicall(calls)
            if not ok:
                # on failure, fall back: set zeros for this group
                for a in group:
                    results[a][f"{sym}_raw"] = 0
                continue
            # decoded[1] is list of returned bytes objects
            ret_list = decoded[1]
            for a, ret in zip(group, ret_list):
                try:
                    if not ret:
                        val = 0
                    else:
                        if isinstance(ret, (bytes, bytearray)):
                            val = int.from_bytes(ret, "big")
                        else:
                            # if web3 returned hex string inside bytes array, be safe:
                            hs = ret.hex() if hasattr(ret, "hex") else (ret if isinstance(ret, str) else "")
                            val = int(hs, 16) if hs else 0
                except Exception:
                    val = 0
                results[a][f"{sym}_raw"] = val

    # Write CSV
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        f.write("address," + ",".join([s + "_raw" for s in syms]) + "\n")
        for a in addresses:
            row = [str(results[a].get(s + "_raw", 0)) for s in syms]
            f.write(a + "," + ",".join(row) + "\n")

    await pool.close()
    return out_path
