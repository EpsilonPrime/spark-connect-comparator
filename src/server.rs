use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    execute_plan_response, AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest,
    AnalyzePlanResponse, ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest,
    ConfigResponse, ExecutePlanRequest, ExecutePlanResponse, InterruptRequest, InterruptResponse,
    ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse,
};

#[derive(Debug, Default)]
pub struct MySparkConnectService {}

#[tonic::async_trait]
impl SparkConnectService for MySparkConnectService {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("Got an execute_plan request: {:?}", request);

        let (tx, rx) = mpsc::channel(128);

        let reply = ExecutePlanResponse {
            session_id: request.into_inner().session_id,
            metrics: None,
            observed_metrics: vec![],
            operation_id: "".to_string(),
            response_id: "".to_string(),
            response_type: execute_plan_response::ResponseType::ResultComplete {
                0: Default::default(),
            }
            .into(),
            schema: None,
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
        println!("Got an analyze_plan request: {:?}", request);

        let reply = AnalyzePlanResponse {
            session_id: request.into_inner().session_id,
            result: None,
        };

        Ok(Response::new(reply))
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        println!("Got a config request: {:?}", request);

        let reply = ConfigResponse {
            session_id: request.into_inner().session_id,
            pairs: vec![],
            warnings: vec![],
        };

        Ok(Response::new(reply))
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

        let reply = InterruptResponse {
            session_id: request.into_inner().session_id,
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
        println!("Got a reattach_execute request: {:?}", request);

        let (tx, rx) = mpsc::channel(128);

        let reply = ExecutePlanResponse {
            session_id: request.into_inner().session_id,
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
        println!("Got a release_execute request: {:?}", request);

        let reply = ReleaseExecuteResponse {
            session_id: request.into_inner().session_id,
            operation_id: None,
        };

        Ok(Response::new(reply))
    }
}
