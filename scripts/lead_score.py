import pandas as pd
import math

def compute_lead_score(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    usd = d['usd_value'].fillna(0).astype(float)
    stable_pct = d.get('stablecoin_pct', 0)
    if not hasattr(stable_pct, 'fillna'):
        stable_pct = pd.Series([stable_pct] * len(d))
    stable_pen = (1.0 - stable_pct.fillna(0)).clip(0, 1)
    ex_pen = (1 - d.get('exchange_deposit_flag', 0).astype(int))

    def log_scale(x):
        try:
            if x <= 0:
                return 0.0
            return min(1.0, math.log10(x) / 6.0)
        except Exception:
            return 0.0

    score = (usd.apply(log_scale) * 70
             + stable_pen * 20
             + ex_pen * 10)
    d['lead_score'] = score.round(1)
    return d
