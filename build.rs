use std::fs;

const OUT_DIR: &str = "src/generated";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = fs::read_dir("./third_party/spark/connector/connect/common/src/main/protobuf/spark/connect")?;

    let mut file_paths: Vec<String> = vec![];

    for file in files {
        let entry = file?.path();
        file_paths.push(entry.to_str().unwrap().to_string());
    }

    let transport = true;

    fs::create_dir_all(OUT_DIR)?;
    tonic_build::configure()
        .out_dir(OUT_DIR)
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