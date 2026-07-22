from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from run_performance_workload import compare_baseline, nearest_rank


def result(query_ns: int, checksum: str = "same") -> dict:
    return {
        "cases": [
            {
                "name": "control",
                "regression_gate": True,
                "query_fingerprint": "f" * 64,
                "result_sha256": checksum,
                "summary": {"query_ns": {"median": query_ns}},
            }
        ]
    }


class PerformanceWorkloadTests(unittest.TestCase):
    def test_nearest_rank(self) -> None:
        samples = [50, 10, 40, 20, 30]
        self.assertEqual(nearest_rank(samples, 0.5), 30)
        self.assertEqual(nearest_rank(samples, 0.95), 50)

    def test_baseline_gate_accepts_threshold(self) -> None:
        comparisons, failures = compare_baseline(result(110), result(100), 10.0)
        self.assertEqual(failures, [])
        self.assertAlmostEqual(comparisons[0]["regression_percent"], 10.0)

    def test_baseline_gate_rejects_regression_and_result_change(self) -> None:
        _, failures = compare_baseline(result(111), result(100), 10.0)
        self.assertEqual(len(failures), 1)
        _, failures = compare_baseline(result(100, "new"), result(100), 10.0)
        self.assertEqual(failures, ["result checksum changed for 'control'"])


if __name__ == "__main__":
    unittest.main()
