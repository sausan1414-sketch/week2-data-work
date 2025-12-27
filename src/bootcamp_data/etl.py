from __future__ import annotations
import json
import logging
from pathlib import Path
import pandas as pd
from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.joins import safe_left_join
from bootcamp_data.transforms import (
    add_missing_flags,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
log = logging.getLogger(__name__)
def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders_raw = read_orders_csv(cfg.raw_orders)
    users_raw = read_users_csv(cfg.raw_users)

    assert_non_empty(orders_raw, name="orders_raw")
    assert_non_empty(users_raw, name="users_raw")

    return orders_raw, users_raw
def transform(orders: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    orders = (
        orders.copy()
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )
    analytics = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
    )
    analytics["amount_winsor"] = winsorize(analytics["amount"])
    analytics = add_outlier_flag(analytics, "amount_winsor")
    return analytics
def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, p) -> None:
    write_parquet(users, p.processed / "users.parquet")
    write_parquet(analytics, p.processed / "analytics_table.parquet")
    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns] + [
        c for c in analytics.columns if c.endswith("_user")
        ]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
def write_run_meta(*, p, orders_raw, users, analytics) -> None:
    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_in_analytics": int(len(analytics)),
        "paths": {
            "raw": str(p.raw),
            "processed": str(p.processed),
            "reports": str(p.reports),
        },
    }
    p.reports.mkdir(parents=True, exist_ok=True)
    with (p.reports / "run_meta.json").open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
def run_etl(root: Path) -> None:
    p = make_paths(root)
    log.info("Loading raw inputs…")
    orders_raw, users_raw = load_inputs(p)
    log.info("Transforming data…")
    analytics = transform(orders_raw, users_raw)
    log.info("Writing outputs…")
    load_outputs(analytics=analytics, users=users_raw, p=p)
    log.info("Writing run metadata…")
    write_run_meta(p=p, orders_raw=orders_raw, users=users_raw, analytics=analytics)
    log.info("ETL run complete.")
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    root = Path(__file__).resolve().parents[1]
    run_etl(root)
if __name__ == "__main__":
    main()

