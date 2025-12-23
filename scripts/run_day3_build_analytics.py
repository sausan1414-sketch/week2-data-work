from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# IMPORTANT: make src importable BEFORE importing bootcamp_data
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize, add_outlier_flag
from bootcamp_data.joins import safe_left_join


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    log = logging.getLogger(__name__)

    p = make_paths(ROOT)

    # 1) Load
    orders = pd.read_parquet(p.processed / "orders_clean.parquet")
    users = pd.read_parquet(p.processed / "users.parquet")

    # 2) Checks
    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"])
    require_columns(users, ["user_id", "country", "signup_date"])

    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    # 3) Time parsing + time parts
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    n_missing_ts = int(orders_t["created_at"].isna().sum())
    log.info("missing created_at after parse: %s / %s", n_missing_ts, len(orders_t))

    # 4) Safe join (orders many -> users one)
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    if len(joined) != len(orders_t):
        raise AssertionError("Row count changed on left join (join explosion?)")

    match_rate = 1.0 - float(joined["country"].isna().mean())
    log.info("rows=%s | country match rate=%.3f", len(joined), match_rate)

    # 5) Outliers (keep rows; cap for viz) + optional flag
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))

    if add_outlier_flag is not None:
        joined = add_outlier_flag(joined, "amount", k=1.5)

    # 6) MINI SUMMARY (Task 7)  ✅ هنا مكانه الصحيح
    summary = (
        joined.groupby("country", dropna=False)
        .agg(n=("order_id", "size"), revenue=("amount", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    print("\n=== Revenue by country ===")
    print(summary)

    # optional: save CSV to reports/
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(reports_dir / "revenue_by_country.csv", index=False)
    log.info("wrote: %s", reports_dir / "revenue_by_country.csv")

    # 7) Write analytics table
    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)
    log.info("wrote: %s", out_path)


if __name__ == "__main__":
    main()
