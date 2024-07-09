use crate::outbound_multiplexer::OutboundMultiplexer;
use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, InterruptRequest, InterruptResponse,
    ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse,
};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug, Default)]
pub struct SparkConnectComparatorService {
    muxer: OutboundMultiplexer,
}

impl SparkConnectComparatorService {
    pub fn new() -> Self {
        SparkConnectComparatorService {
            muxer: OutboundMultiplexer::new(),
        }
    }
}

#[tonic::async_trait]
impl SparkConnectService for SparkConnectComparatorService {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("Got an execute_plan request: {:?}", request);

        let (tx, rx) = mpsc::channel(128);

        let req_ref = request.get_ref();

        let mut client = self
            .muxer
            .get_client(req_ref.session_id.to_string())
            .await
            .unwrap();

        let mut stream = client.execute_plan(request).await?.into_inner();

        while let Some(reply) = stream.next().await {
            match tx.send(reply).await {
                Ok(_) => {
                    // item (server response) was queued to be sent to client
                }
                Err(_item) => {
                    // output_stream was build from rx and both are dropped
                }
            }
        }

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ExecutePlanStream
        ))
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        println!("Got an analyze_plan request: {:?}", request);

        let req_ref = request.get_ref();

        let client = self.muxer.get_client(req_ref.session_id.to_string());

        client.await.unwrap().analyze_plan(request).await
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        println!("Got a config request: {:?}", request);

        let req_ref = request.get_ref();

        let client = self.muxer.get_client(req_ref.session_id.to_string());

        client.await.unwrap().config(request).await
    }

    async fn add_artifacts(
        &self,
        request: Request<Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        println!("Got an add_artifacts request: {:?}", request);

        let reply = AddArtifactsResponse { artifacts: vec![] };

        Ok(Response::new(reply))
    }

    async fn artifact_status(
        &self,
        request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        println!("Got an artifact_status request: {:?}", request);

        let reply = ArtifactStatusesResponse {
            statuses: Default::default(),
        };

        Ok(Response::new(reply))
    }

    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        println!("Got an interrupt request: {:?}", request);

        let req_ref = request.get_ref();

        let client = self.muxer.get_client(req_ref.session_id.to_string());

        client.await.unwrap().interrupt(request).await
    }

    type ReattachExecuteStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("Got a reattach_execute request: {:?}", request);

        let (tx, rx) = mpsc::channel(128);

        let req_ref = request.get_ref();

        let mut client = self
            .muxer
            .get_client(req_ref.session_id.to_string())
            .await
            .unwrap();

        let mut stream = client.reattach_execute(request).await?.into_inner();

        while let Some(reply) = stream.next().await {
            match tx.send(reply).await {
                Ok(_) => {
                    // item (server response) was queued to be sent to client
                }
                Err(_item) => {
                    // output_stream was build from rx and both are dropped
                }
            }
        }

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ExecutePlanStream
        ))
    }

    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        println!("Got a release_execute request: {:?}", request);

        let req_ref = request.get_ref();

        let client = self.muxer.get_client(req_ref.session_id.to_string());

        client.await.unwrap().release_execute(request).await
    }
}
