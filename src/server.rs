use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, FetchErrorDetailsRequest, FetchErrorDetailsResponse,
    InterruptRequest, InterruptResponse, ReattachExecuteRequest, ReleaseExecuteRequest,
    ReleaseExecuteResponse, ReleaseSessionRequest, ReleaseSessionResponse,
};

#[derive(Debug, Default)]
pub struct MySparkConnectService {}

impl MySparkConnectService {
    pub(crate) fn new(_server: MySparkConnectService) -> Self {
        Self{}
    }
}

#[tonic::async_trait]
impl SparkConnectService for MySparkConnectService {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("Got a request: {:?}", request);

        let (tx, rx) = mpsc::channel(128);

        let reply = ExecutePlanResponse {
            session_id: request.into_inner().session_id,
            metrics: None,
            observed_metrics: vec![],
            operation_id: "".to_string(),
            response_id: "".to_string(),
            response_type: None,
            schema: None,
            server_side_session_id: "".to_string(),
        };
        match tx.send(Result::<_, Status>::Ok(reply)).await {
            Ok(_) => {
                // item (server response) was queued to be sent to client
            }
            Err(_item) => {
                // output_stream was build from rx and both are dropped
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
        println!("Got a request: {:?}", request);

        let reply = AnalyzePlanResponse {
            session_id: request.into_inner().session_id,
            server_side_session_id: "".to_string(),
            result: None,
        };

        Ok(Response::new(reply))
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = ConfigResponse {
            session_id: request.into_inner().session_id,
            server_side_session_id: "".to_string(),
            pairs: vec![],
            warnings: vec![],
        };

        Ok(Response::new(reply))
    }

    async fn add_artifacts(
        &self,
        request: Request<Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = AddArtifactsResponse {
            session_id: "".to_string(),
            server_side_session_id: "".to_string(),
            artifacts: vec![],
        };

        Ok(Response::new(reply))
    }

    async fn artifact_status(
        &self,
        request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = ArtifactStatusesResponse {
            session_id: "".to_string(),
            server_side_session_id: "".to_string(),
            statuses: Default::default(),
        };

        Ok(Response::new(reply))
    }

    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = InterruptResponse {
            session_id: "".to_string(),
            server_side_session_id: "".to_string(),
            interrupted_ids: vec![],
        };

        Ok(Response::new(reply))
    }

    type ReattachExecuteStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("Got a request: {:?}", request);

        let (tx, rx) = mpsc::channel(128);

        let reply = ExecutePlanResponse {
            session_id: "".to_string(),
            server_side_session_id: "".to_string(),
            operation_id: "".to_string(),
            response_id: "".to_string(),
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: None,
        };
        match tx.send(Result::<_, Status>::Ok(reply)).await {
            Ok(_) => {
                // item (server response) was queued to be sent to client
            }
            Err(_item) => {
                // output_stream was build from rx and both are dropped
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
        println!("Got a request: {:?}", request);

        let reply = ReleaseExecuteResponse {
            session_id: "".to_string(),
            server_side_session_id: "".to_string(),
            operation_id: None,
        };

        Ok(Response::new(reply))
    }

    async fn release_session(
        &self,
        request: Request<ReleaseSessionRequest>,
    ) -> Result<Response<ReleaseSessionResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = ReleaseSessionResponse {
            session_id: "".to_string(),
            server_side_session_id: "".to_string(),
        };

        Ok(Response::new(reply))
    }

    async fn fetch_error_details(
        &self,
        request: Request<FetchErrorDetailsRequest>,
    ) -> Result<Response<FetchErrorDetailsResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = FetchErrorDetailsResponse {
            session_id: "".to_string(),
            root_error_idx: None,
            server_side_session_id: "".to_string(),
            errors: vec![],
        };

        Ok(Response::new(reply))
    }
}
