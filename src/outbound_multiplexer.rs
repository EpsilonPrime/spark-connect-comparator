use crate::proto::spark_connect_service_client::SparkConnectServiceClient;
use std::collections::HashMap;

type OutboundClient = SparkConnectServiceClient<tonic::transport::Channel>;

#[derive(Debug, Default)]
pub struct OutboundMultiplexer {
    destination: String,
    clients: HashMap<String, OutboundClient>,
}

impl OutboundMultiplexer {
    pub fn new() -> Self {
        OutboundMultiplexer {
            // TODO -- Make the destination configurable.
            // All clients in this multiplexer share the same destination.
            destination: "http://[::1]:50051".to_string(),
            clients: HashMap::new(),
        }
    }

    fn add_client(&mut self, session_id: String, client: OutboundClient) {
        self.clients.insert(session_id, client);
    }

    pub async fn get_client(
        &mut self,
        session_id: &String,
    ) -> Result<OutboundClient, tonic::transport::Error> {
        if self.clients.contains_key(session_id) {
            return Ok(self.clients.get(session_id).unwrap().clone());
        }
        let connection = SparkConnectServiceClient::connect(self.destination.to_string());

        // TODO -- Figure out how to do this without waiting for the connection.
        self.add_client(session_id.clone(), connection.await?);

        return Ok(self.clients.get(session_id).unwrap().clone());
    }

    pub fn remove_client(&mut self, session_id: &String) {
        self.clients.remove(session_id);
    }
}
