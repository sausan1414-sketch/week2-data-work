import pandas as pd

def require_columns(df, cols):
    missing = [col for col in cols if col not in df.columns]
    assert missing == [], f"Missing columns: {missing}"

def assert_in_range(s: pd.Series, *, lo=None, hi=None, name: str = "value") -> None:
    x = s.dropna()

    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"

    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"

def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    return (
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / n)
        .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()

    for c in cols:
        out[f"{c}__isna"] = out[c].isna()

    return out

def assert_unique_key(df, key, *, allow_na=False):
    if not allow_na:
        assert df[key].notna().all(), f"{key} contains NA"

    dup = df[key].duplicated(keep=False) & df[key].notna()
    assert not dup.any(), f"{key} not unique; {dup.sum()} duplicate rows"

def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    assert len(df) > 0, f"{name} has 0 rows"