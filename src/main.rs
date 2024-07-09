use crate::{
    proto::spark_connect_service_server::SparkConnectServiceServer,
    server::SparkConnectComparatorService,
};
use tonic::transport::Server;

mod outbound_multiplexer;
mod server;

#[allow(clippy::all)]
mod proto {
    tonic::include_proto!("spark.connect");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("spark_connect");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50055".parse()?;
    let server = SparkConnectComparatorService::new();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(SparkConnectServiceServer::new(server))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
