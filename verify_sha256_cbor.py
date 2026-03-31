#!/usr/bin/env python3
"""
Cross-language verification script for Sha256Cbor hash algorithm.

Run with: PYTHONHASHSEED=0 python3 verify_sha256_cbor.py

This script computes the same hash values as the Rust Sha256Cbor implementation
in cedfs-kv/src/hash.rs, using Python's cbor2 + hashlib.sha256.

Expected output should match the Rust test vectors printed by
test_sha256_cbor_print_test_vectors.
"""

import hashlib

try:
    import cbor2
except ImportError:
    print("ERROR: cbor2 not installed. Run: pip install cbor2")
    raise SystemExit(1)


def sha256_cbor(input_val):
    """Hash function: SHA256(cbor2.dumps(input))"""
    cbor_bytes = cbor2.dumps(input_val)
    return hashlib.sha256(cbor_bytes).digest()


def main():
    print("=" * 60)
    print("Sha256Cbor Cross-Language Verification (Python)")
    print("=" * 60)

    # 1. NONE_HASH = hash_func(None)
    none_cbor = cbor2.dumps(None)
    print(f"\ncbor2.dumps(None) = {none_cbor.hex()} (expect: f6)")
    assert none_cbor == b'\xf6', f"CBOR None encoding mismatch: {none_cbor.hex()}"

    none_hash = sha256_cbor(None)
    print(f"NONE_HASH = {none_hash.hex()}")
    assert none_hash.hex() == "b0b2988b6bbe724bacda5e9e524736de0bc7dae41c46b4213c50e1d35d4e5f13"

    # 2. hash((NONE_HASH, (1, 2, 3), None))
    hash1 = sha256_cbor((none_hash, (1, 2, 3), None))
    print(f"hash((NONE_HASH, (1,2,3), None)) = {hash1.hex()}")

    # Verify CBOR encoding structure
    cbor_bytes = cbor2.dumps((none_hash, (1, 2, 3), None))
    print(f"  CBOR bytes ({len(cbor_bytes)} bytes): {cbor_bytes[:6].hex()}...{cbor_bytes[-4:].hex()}")
    assert cbor_bytes[0] == 0x83, "Outer array header"
    assert cbor_bytes[1] == 0x58, "Byte string, 1-byte length"
    assert cbor_bytes[2] == 0x20, "Byte string length = 32"
    assert cbor_bytes[35] == 0x83, "Inner array header"
    assert cbor_bytes[36] == 0x01, "Token 1"
    assert cbor_bytes[37] == 0x02, "Token 2"
    assert cbor_bytes[38] == 0x03, "Token 3"
    assert cbor_bytes[39] == 0xf6, "Null for extra_keys"

    # 3. hash((hash1, (1, 2, 3), None))
    hash2 = sha256_cbor((hash1, (1, 2, 3), None))
    print(f"hash((hash1, (1,2,3), None)) = {hash2.hex()}")

    # 4. hash((NONE_HASH, (100, 200, 300, 65536), None))
    hash3 = sha256_cbor((none_hash, (100, 200, 300, 65536), None))
    print(f"hash((NONE_HASH, (100,200,300,65536), None)) = {hash3.hex()}")

    # 5. hash((NONE_HASH, (1, 2, 3), ('image_hash:abc123',)))
    hash4 = sha256_cbor((none_hash, (1, 2, 3), ('image_hash:abc123',)))
    print(f"hash((NONE_HASH, (1,2,3), ('image_hash:abc123',))) = {hash4.hex()}")

    # 6. Iterative block hashing: blocks [1,2], [3,4], [5,6]
    print(f"\n--- Iterative block hashing ---")
    current_hash = none_hash
    blocks = [(1, 2), (3, 4), (5, 6)]
    for i, block in enumerate(blocks):
        current_hash = sha256_cbor((current_hash, block, None))
        print(f"  After block {block}: {current_hash.hex()}")

    print(f"\n{'=' * 60}")
    print("All assertions passed!")
    print("Compare these values with Rust test output:")
    print("  cargo test --package cedfs-kv --lib hash::tests::test_sha256_cbor_print_test_vectors -- --nocapture")
    print("=" * 60)


if __name__ == "__main__":
    main()
