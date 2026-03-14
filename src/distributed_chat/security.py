from __future__ import annotations

import hashlib
import secrets


def derive_room_key(room_name: str, password: str) -> str:
    payload = f"{room_name}:{password}".encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", payload, room_name.encode("utf-8"), 120_000).hex()


def generate_nonce() -> str:
    return secrets.token_hex(16)


def build_join_proof(room_key: str, nonce: str) -> str:
    return hashlib.sha256(f"{nonce}:{room_key}".encode("utf-8")).hexdigest()


def verify_join_proof(room_key: str, nonce: str, proof: str) -> bool:
    return secrets.compare_digest(build_join_proof(room_key, nonce), proof)
