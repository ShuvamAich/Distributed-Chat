from __future__ import annotations

import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from distributed_chat.config import parse_seed_peers
from distributed_chat.order import LamportClock, VectorClock


class OrderingTests(unittest.TestCase):
    def test_lamport_tick_merges_remote_value(self) -> None:
        clock = LamportClock()
        self.assertEqual(clock.tick(), 1)
        self.assertEqual(clock.tick(5), 6)

    def test_vector_clock_merge_and_advance(self) -> None:
        clock = VectorClock()
        clock.advance("A")
        clock.merge({"A": 1, "B": 3})
        clock.advance("B")
        self.assertEqual(clock["A"], 1)
        self.assertEqual(clock["B"], 4)

    def test_parse_seed_peers(self) -> None:
        self.assertEqual(parse_seed_peers("10.0.0.10:6000,10.0.0.11:6001"), (("10.0.0.10", 6000), ("10.0.0.11", 6001)))


if __name__ == "__main__":
    unittest.main()
