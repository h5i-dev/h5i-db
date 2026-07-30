from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "backtest_compare"))

from run import h5i_result, summarize, validate

WORKLOAD = {
    "name": "test",
    "quote_events": 200,
    "signals": 2,
}


class BacktestComparisonTests(unittest.TestCase):
    def test_h5i_result_maps_distinct_boundaries(self) -> None:
        result = h5i_result(
            {
                "config": {
                    "book_events": 200,
                    "trades": 0,
                    "signals": 2,
                },
                "derived": {
                    "kernel_ms": 10,
                    "decode_ms": 4,
                    "run_ms": 25,
                },
            },
            "abc123",
            "test",
        )
        self.assertEqual(result["event_count"], 200)
        self.assertEqual(result["orders_submitted"], 2)
        self.assertEqual(result["throughput_events_per_sec"], 20_000)
        self.assertEqual(result["timings_ms"]["persisted_run"], 25)

    def test_validate_rejects_incomplete_engine_run(self) -> None:
        result = {
            "engine": "example",
            "event_count": 199,
            "events_seen": 199,
            "orders_submitted": 2,
        }
        with self.assertRaisesRegex(ValueError, "processed 199 events"):
            validate(result, WORKLOAD)

    def test_summary_uses_median_and_preserves_boundary(self) -> None:
        samples = [
            {
                "engine": "example",
                "engine_version": "1",
                "boundary": "engine",
                "timings_ms": {"engine": elapsed},
                "throughput_events_per_sec": throughput,
            }
            for elapsed, throughput in ((30, 3), (10, 1), (20, 2))
        ]
        result = summarize(samples)
        self.assertEqual(result["median_engine_ms"], 20)
        self.assertEqual(result["median_events_per_sec"], 2)
        self.assertEqual(result["boundary"], "engine")


if __name__ == "__main__":
    unittest.main()
