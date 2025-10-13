use std::fs;
use std::path::Path;

fn main() {
    for entry in fs::read_dir(Path::new("proto")).unwrap() {
        let entry = entry.unwrap();
        let file_name = entry.file_name();
        let proto_name = file_name.to_str().unwrap();
        tonic_build::configure()
            .protoc_arg("--experimental_allow_proto3_optional")
            .out_dir("src")
            .compile_protos(&[proto_name], &["proto"])
            .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
    }
}
