import pandas as pd
from pathlib import Path
from web3 import Web3

BUILTIN_EXCHANGES = {
    '0x742d35Cc6634C0532925a3b844Bc454e4438f44e',
    '0xDC76CD25977E0a5Ae17155770273aD58648900D3',
}

CONFIG_PATH = Path('data/config/exchanges.csv')


def load_exchange_set():
    s = set(BUILTIN_EXCHANGES)
    if CONFIG_PATH.exists():
        try:
            import csv
            with CONFIG_PATH.open('r') as f:
                for row in csv.reader(f):
                    if not row: continue
                    try:
                        s.add(Web3.toChecksumAddress(row[0].strip()))
                    except Exception:
                        pass
        except Exception:
            pass
    return s


def detect_exchange_inflow(df: pd.DataFrame, exchange_set: set) -> pd.DataFrame:
    d = df.copy()
    def flag(a):
        try:
            return 1 if Web3.toChecksumAddress(a) in exchange_set else 0
        except Exception:
            return 0
    d['exchange_deposit_flag'] = d['address'].apply(flag).astype(int)
    return d
