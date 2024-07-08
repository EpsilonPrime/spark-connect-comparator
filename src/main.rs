mod server;
pub mod spark {
    pub mod connect {
        #![allow(clippy::all)]
        include!("generated/spark.connect.rs");
    }
}

use crate::server::MySparkConnectService;
use tonic::transport::Server;
use crate::spark::connect::spark_connect_service_server::SparkConnectServiceServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50055".parse()?;
    let server = MySparkConnectService::default();

    let reflection_service = tonic_reflection::server::Builder::configure()
        //.register_encoded_file_descriptor_set(spark_connect::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    Server::builder()
        .add_service(SparkConnectServiceServer::new(server))
        .add_service(reflection_service)
        .serve(addr)
        .await?;

    Ok(())
}
