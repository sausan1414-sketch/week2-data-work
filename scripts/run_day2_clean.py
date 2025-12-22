from pathlib import Path
import sys
import logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
    assert_in_range,
)
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    paths = make_paths(ROOT)

    # Load raw data
    orders = read_orders_csv(paths.raw / "orders.csv")
    users = read_users_csv(paths.raw / "users.csv")

    # Checks
    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "status", "created_at"],
    )
    require_columns(users, ["user_id"])
    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    #  Enforce schema (IMPORTANT)
    orders = enforce_schema(orders)

    # Range check
    assert_in_range(orders["amount"], lo=0, name="amount")

    # Missingness report
    report = missingness_report(orders)
    paths.reports.mkdir(parents=True, exist_ok=True)
    report.to_csv(
        paths.reports / "missingness_orders.csv",
        sep=";"
    )

    # Cleaning
    orders = orders.assign(
        status_norm=normalize_text(orders["status"])
    )

    mapping = {
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
    }

    orders = orders.assign(
        status_clean=apply_mapping(orders["status_norm"], mapping)
    )

    orders = add_missing_flags(orders, ["amount", "quantity"])

    # Write output
    write_parquet(orders, paths.processed / "orders_clean.parquet")
    log.info("orders_clean.parquet written successfully")


if __name__ == "__main__":
    main()