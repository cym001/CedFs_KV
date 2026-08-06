fn main() {
    const V1_PROTOS: &[&str] = &["proto/kvcache.proto", "proto/kvserver.proto"];
    const V2_PROTOS: &[&str] = &["proto/kvcache_v2.proto", "proto/kvserver_v2.proto"];

    for proto in V1_PROTOS.iter().chain(V2_PROTOS.iter()) {
        println!("cargo:rerun-if-changed={proto}");
    }

    // Keep the checked-in V1 bindings stable during the migration. V2 bindings
    // are generated into OUT_DIR and must never be edited by hand.
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src")
        .compile_protos(V1_PROTOS, &["proto"])
        .unwrap_or_else(|e| panic!("failed to compile V1 protos: {e}"));

    let descriptor_path = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap())
        .join("cedfs_kv_v2_descriptor.bin");
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(V2_PROTOS, &["proto"])
        .unwrap_or_else(|e| panic!("failed to compile V2 protos: {e}"));
}
