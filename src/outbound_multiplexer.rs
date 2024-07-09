use crate::spark::connect::spark_connect_service_client::SparkConnectServiceClient;

#[derive(Debug, Default)]
pub struct OutboundMultiplexer {
    destination: String,
}

impl OutboundMultiplexer {
    pub fn new() -> Self {
        OutboundMultiplexer {
            // TODO -- Make the destination configurable.
            destination: "http://[::1]:50051".to_string(),
        }
    }

    pub async fn get_client(
        &self,
        _session_id: String,
    ) -> Result<SparkConnectServiceClient<tonic::transport::Channel>, tonic::transport::Error> {
        // TODO -- Reuse the client for the specified session (utilize shared futures).
        let connection = SparkConnectServiceClient::connect(self.destination.to_string());

        Ok(connection.await?)
    }
}
