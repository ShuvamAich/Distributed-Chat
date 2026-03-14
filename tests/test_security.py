from __future__ import annotations

import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from distributed_chat.security import build_join_proof, derive_room_key, verify_join_proof


class SecurityTests(unittest.TestCase):
    def test_join_proof_round_trip(self) -> None:
        room_key = derive_room_key("demo-room", "secret")
        proof = build_join_proof(room_key, "nonce")
        self.assertTrue(verify_join_proof(room_key, "nonce", proof))
        self.assertFalse(verify_join_proof(room_key, "other", proof))


if __name__ == "__main__":
    unittest.main()
