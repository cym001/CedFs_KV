# Deployment Notes

## PYTHONHASHSEED requirement for `sha256_cbor`

To align Rust-side block hash initialization with vLLM (`init_none_hash` in vLLM >= PR#20511),
set a fixed `PYTHONHASHSEED` value for all cache-sharing processes.

Recommended value:

```bash
export PYTHONHASHSEED=0
```

Behavior in this repository:

- when `hash_algorithm = "sha256_cbor"` and `PYTHONHASHSEED` is set,
  `NONE_HASH` is initialized as `sha256(cbor(PYTHONHASHSEED_string))`;
- when `hash_algorithm = "sha256_cbor"` and `PYTHONHASHSEED` is not set,
  server initialization fails fast to avoid non-reproducible cache keys.

Make sure the same `PYTHONHASHSEED` is configured across all nodes that share
prefix-cache metadata.
