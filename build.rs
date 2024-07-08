use std::path::PathBuf;
use std::{env, fs};

const OUT_DIR: &str = "src/generated";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = fs::read_dir(
        "./third_party/spark/connector/connect/common/src/main/protobuf/spark/connect",
    )?;

    let mut file_paths: Vec<String> = vec![];

    for file in files {
        let entry = file?.path();
        file_paths.push(entry.to_str().unwrap().to_string());
    }

    let transport = true;

    fs::create_dir_all(OUT_DIR)?;
    let descriptor_dir_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("generated");
    fs::create_dir_all(&descriptor_dir_path)?;
    let descriptor_path = descriptor_dir_path.join("spark_connect.bin");
    tonic_build::configure()
        .out_dir(OUT_DIR)
        .file_descriptor_set_path(&descriptor_path)
        .protoc_arg("--experimental_allow_proto3_optional")
        .build_server(true)
        .build_client(true)
        .build_transport(transport)
        .compile(
            file_paths.as_ref(),
            &["./third_party/spark/connector/connect/common/src/main/protobuf"],
        )?;

    Ok(())
}
