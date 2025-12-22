import re
import pandas as pd

_ws = re.compile(r"\s+")


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    return (
        df.isna()
        .sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / n)
        .sort_values("p_missing", ascending=False)
    )


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}_isna"] = out[c].isna()
    return out


def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )


def apply_mapping(s: pd.Series, mapping: dict) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x))






#import pandas as pd

#def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
#    return df.assign(
#        order_id=df["order_id"].astype("string"),
#        user_id=df["user_id"].astype("string"),
#        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
#        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
#    )

