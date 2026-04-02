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
    """Hash function: SHA256(cbor2.dumps(input, canonical=True))."""
    cbor_bytes = cbor2.dumps(input_val, canonical=True)
    return hashlib.sha256(cbor_bytes).digest()


def main():
    print("=" * 60)
    print("Sha256Cbor Cross-Language Verification (Python)")
    print("=" * 60)

    # 1. NONE_HASH = hash_func(PYTHONHASHSEED)
    # vLLM >= PR#20511: init_none_hash(hash_fn) uses hash_fn(hash_seed_string)
    hash_seed = "0"
    none_cbor = cbor2.dumps(hash_seed, canonical=True)
    print(f"\ncbor2.dumps('0', canonical=True) = {none_cbor.hex()} (expect: 6130)")
    assert none_cbor == b"\x61\x30", f"CBOR seed encoding mismatch: {none_cbor.hex()}"

    none_hash = sha256_cbor(hash_seed)
    print(f"NONE_HASH = {none_hash.hex()}")
    assert none_hash.hex() == "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e"

    # 2. hash((int.from_bytes(NONE_HASH,'big'), (1, 2, 3), ()))
    canon_prefix = int.from_bytes(none_hash, byteorder="big", signed=False)
    hash1 = sha256_cbor((canon_prefix, (1, 2, 3), ()))
    print(f"hash((canon_prefix, (1,2,3), ())) = {hash1.hex()}")
    assert hash1.hex() == "f05cc9eb85766390ac54a56f956bde6ad4fd0d0d9465d7fd1e88ab61ca7a31c4"

    # Verify CBOR encoding structure
    cbor_bytes = cbor2.dumps((canon_prefix, (1, 2, 3), ()), canonical=True)
    print(f"  CBOR bytes ({len(cbor_bytes)} bytes): {cbor_bytes[:6].hex()}...{cbor_bytes[-4:].hex()}")
    assert cbor_bytes[0] == 0x83, "Outer array header"
    assert cbor_bytes[-5] == 0x83, "Tokens array header"
    assert cbor_bytes[-4] == 0x01, "Token 1"
    assert cbor_bytes[-3] == 0x02, "Token 2"
    assert cbor_bytes[-2] == 0x03, "Token 3"
    assert cbor_bytes[-1] == 0x80, "Empty tuple for extra_keys"

    # 3. hash((int.from_bytes(hash1,'big'), (1, 2, 3), ()))
    hash2 = sha256_cbor((int.from_bytes(hash1, "big"), (1, 2, 3), ()))
    print(f"hash((hash1_as_int, (1,2,3), ())) = {hash2.hex()}")
    assert hash2.hex() == "2ae4de831eba33f8e9977f0ca1ae0718ef455be797864c4cfd426c4c2c4d41dc"

    # 4. hash((canon_prefix, (100, 200, 300, 65536), ()))
    hash3 = sha256_cbor((canon_prefix, (100, 200, 300, 65536), ()))
    print(f"hash((canon_prefix, (100,200,300,65536), ())) = {hash3.hex()}")
    assert hash3.hex() == "b7ffbdfbd8edaf74c1401c9773559d9fab3e5b0cc32399dd8e8ebda21cae1399"

    # 5. hash((canon_prefix, (1, 2, 3), ('image_hash:abc123',)))
    hash4 = sha256_cbor((canon_prefix, (1, 2, 3), ('image_hash:abc123',)))
    print(f"hash((canon_prefix, (1,2,3), ('image_hash:abc123',))) = {hash4.hex()}")
    assert hash4.hex() == "b13bcd05a97c5b22c69a302305912db74382ac70a4075aa31df8750745ed85af"

    # 6. Iterative block hashing: blocks [1,2], [3,4], [5,6]
    print("\n--- Iterative block hashing ---")
    current_hash = none_hash
    blocks = [(1, 2), (3, 4), (5, 6)]
    for i, block in enumerate(blocks):
        current_hash = sha256_cbor((int.from_bytes(current_hash, "big"), block, ()))
        print(f"  After block {block}: {current_hash.hex()}")

    print(f"\n{'=' * 60}")
    print("All assertions passed!")
    print("Compare these values with Rust test output:")
    print("  cargo test --package cedfs-kv --lib hash::tests::test_sha256_cbor_print_test_vectors -- --nocapture")
    print("=" * 60)


if __name__ == "__main__":
    main()
