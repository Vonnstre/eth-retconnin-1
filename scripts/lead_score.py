# lead_score.py
import pandas as pd

def compute_lead_score(df: pd.DataFrame) -> pd.DataFrame:
    """Simple, transparent scoring: scale USD, penalize stablecoin-only, and exchanges.
    Columns required: usd_value, stablecoin_pct, exchange_deposit_flag (bool/int)
    """
    d = df.copy()
    usd = d['usd_value'].fillna(0).astype(float)
    stable_pen = (1.0 - d.get('stablecoin_pct', 0).fillna(0)).clip(0, 1)
    ex_pen = (1 - d.get('exchange_deposit_flag', 0).astype(int))  # 0 if exchange, 1 otherwise
    # Scale: log to compress, then 0..100
    score = (usd.apply(lambda x: 0 if x <= 0 else min(1.0, (__import__('math').log10(x) / 6.0))) * 70
             + stable_pen * 20
             + ex_pen * 10)
    d['lead_score'] = score.round(1)
    return d
