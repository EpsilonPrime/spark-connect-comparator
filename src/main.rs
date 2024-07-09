mod server;
mod outbound_multiplexer;

pub mod spark {
    pub mod connect {
        #![allow(clippy::all)]
        include!("generated/spark.connect.rs");
    }

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("generated/spark_connect");
}

use crate::server::MySparkConnectService;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;
use tonic::transport::Server;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50055".parse()?;
    let server = MySparkConnectService::new();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(spark::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(SparkConnectServiceServer::new(server))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
