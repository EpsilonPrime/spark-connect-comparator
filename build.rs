use std::{env, io::Error, path::PathBuf};

fn main() -> Result<(), Error> {
    let proto_root =
        PathBuf::from("./third_party/spark/connector/connect/common/src/main/protobuf");
    let spark_connect = PathBuf::from("spark/connect");

    let protos = [
        "base.proto",
        "catalog.proto",
        "commands.proto",
        "common.proto",
        "expressions.proto",
        "relations.proto",
        "types.proto",
    ]
    .map(|file| spark_connect.join(file));

    let descriptor_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("spark_connect.bin");
    tonic_build::configure()
        .file_descriptor_set_path(descriptor_path)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&protos, &[proto_root])
}
