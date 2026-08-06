# GlobalKV proto ownership and generation

`cedfs-proto/proto` is the only source of truth for the GlobalKV wire protocol.
The files under `cedfs-proto/src` are legacy V1 Rust outputs; V2 Rust bindings and
the V2 descriptor set are generated into Cargo `OUT_DIR` and must not be edited.

Python bindings in the LMCache `kv_transfer` plugin must be generated directly
from this directory. A copied `.proto` file in LMCache is not an accepted input.
The fixed input order is:

1. `kvcache.proto`
2. `kvserver.proto`
3. `kvcache_v2.proto`
4. `kvserver_v2.proto`

The V2 capability response exposes the SHA-256 digest of the exact descriptor set
embedded by the Rust server. Generated Python clients must compute the digest of
their descriptor set and reject V2 operation when it differs. CI must regenerate
both language bindings in a clean tree and fail when tracked generated outputs
change or when the two descriptor digests differ.

The phase-A LMCache client only calls `GetCapabilities` as an opaque gRPC probe.
It deliberately does not parse or trust V2 messages until generated bindings and
the descriptor checksum gate are enabled in the next implementation phase.
