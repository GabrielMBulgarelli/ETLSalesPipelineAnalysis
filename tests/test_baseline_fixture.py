import json
import unittest
from pathlib import Path

from scripts.baseline_fixture import BATCH_TIMESTAMP, build_baseline_snapshot


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "contracts" / "fixtures"
EXPECTED = ROOT / "contracts" / "expected" / "baseline_snapshot.json"


class BaselineFixtureTests(unittest.TestCase):
    def test_fixture_covers_required_edge_cases(self):
        snapshot = build_baseline_snapshot(RAW, BATCH_TIMESTAMP)

        self.assertEqual(snapshot["processed"]["customers"]["duplicates_removed"], 1)
        self.assertEqual(snapshot["processed"]["order_items"]["duplicates_removed"], 1)
        self.assertEqual(snapshot["quality"]["rules"]["orphan_order_item"], 1)
        self.assertEqual(snapshot["quality"]["rules"]["orphan_review"], 1)
        self.assertEqual(snapshot["quality"]["rules"]["invalid_review_score"], 1)
        self.assertEqual(snapshot["metrics"]["sales_fact_rows"], 3)
        self.assertEqual(snapshot["metrics"]["unique_sales_orders"], 2)
        self.assertEqual(snapshot["metrics"]["same_state_items"], 1)
        self.assertEqual(snapshot["metrics"]["cross_state_items"], 2)
        self.assertEqual(snapshot["metrics"]["delayed_items"], 1)
        self.assertEqual(snapshot["metrics"]["gross_item_value"], "355.00")

    def test_primary_payment_is_deterministic(self):
        snapshot = build_baseline_snapshot(RAW, BATCH_TIMESTAMP)
        rows = snapshot["curated"]["fact_sales"]

        o1_types = {row["PaymentType"] for row in rows if row["OrderID"] == "o1"}
        self.assertEqual(o1_types, {"credit_card"})

    def test_warehouse_facts_use_surrogate_keys(self):
        snapshot = build_baseline_snapshot(RAW, BATCH_TIMESTAMP)

        for row in snapshot["warehouse"]["fact_sales"]:
            self.assertNotIn("CustomerID", row)
            self.assertIsInstance(row["CustomerKey"], int)
            self.assertIsInstance(row["ProductKey"], int)
            self.assertIsInstance(row["SellerKey"], int)
            self.assertIsInstance(row["StatusKey"], int)

    def test_snapshot_matches_versioned_expectation(self):
        actual = build_baseline_snapshot(RAW, BATCH_TIMESTAMP)
        expected = json.loads(EXPECTED.read_text(encoding="utf-8"))

        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
