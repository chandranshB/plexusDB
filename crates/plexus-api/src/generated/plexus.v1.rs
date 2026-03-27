// Pre-generated tonic stubs matching proto/plexus.proto
// To regenerate from source: install protoc, then run `cargo build -p plexus-api`
// These stubs are committed so protoc is not required to build the project.

#[derive(Clone,Copy,Debug,PartialEq,Eq,Hash,PartialOrd,Ord,::prost::Enumeration)]
#[repr(i32)]
pub enum ConsistencyLevel { Unspecified=0, Strong=1, Eventual=2, Session=3 }

#[derive(Clone,Copy,Debug,PartialEq,Eq,Hash,PartialOrd,Ord,::prost::Enumeration)]
#[repr(i32)]
pub enum NodeState { Unspecified=0, Leader=1, Follower=2, Candidate=3, Learner=4, Offline=5 }

#[derive(Clone,PartialEq,::prost::Message)]
pub struct PutRequest {
    #[prost(bytes="vec",tag="1")] pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec",tag="2")] pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64,tag="3")] pub ttl_seconds: u64,
    #[prost(string,tag="4")] pub namespace: ::prost::alloc::string::String,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct PutResponse {
    #[prost(bool,tag="1")] pub success: bool,
    #[prost(uint64,tag="2")] pub timestamp: u64,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct GetRequest {
    #[prost(bytes="vec",tag="1")] pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(string,tag="2")] pub namespace: ::prost::alloc::string::String,
    #[prost(enumeration="ConsistencyLevel",tag="3")] pub consistency: i32,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct GetResponse {
    #[prost(bool,tag="1")] pub found: bool,
    #[prost(bytes="vec",tag="2")] pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64,tag="3")] pub timestamp: u64,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct DeleteRequest {
    #[prost(bytes="vec",tag="1")] pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(string,tag="2")] pub namespace: ::prost::alloc::string::String,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct DeleteResponse {
    #[prost(bool,tag="1")] pub success: bool,
    #[prost(uint64,tag="2")] pub timestamp: u64,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct BatchPutRequest { #[prost(message,repeated,tag="1")] pub entries: ::prost::alloc::vec::Vec<PutRequest> }
#[derive(Clone,PartialEq,::prost::Message)]
pub struct BatchPutResponse {
    #[prost(uint32,tag="1")] pub succeeded: u32,
    #[prost(uint32,tag="2")] pub failed: u32,
    #[prost(string,repeated,tag="3")] pub errors: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct BatchGetRequest { #[prost(message,repeated,tag="1")] pub keys: ::prost::alloc::vec::Vec<GetRequest> }
#[derive(Clone,PartialEq,::prost::Message)]
pub struct BatchGetResponse { #[prost(message,repeated,tag="1")] pub results: ::prost::alloc::vec::Vec<GetResponse> }
#[derive(Clone,PartialEq,::prost::Message)]
pub struct BatchDeleteRequest { #[prost(message,repeated,tag="1")] pub keys: ::prost::alloc::vec::Vec<DeleteRequest> }
#[derive(Clone,PartialEq,::prost::Message)]
pub struct BatchDeleteResponse {
    #[prost(uint32,tag="1")] pub succeeded: u32,
    #[prost(uint32,tag="2")] pub failed: u32,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct ScanRequest {
    #[prost(bytes="vec",tag="1")] pub start_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec",tag="2")] pub end_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32,tag="3")] pub limit: u32,
    #[prost(string,tag="4")] pub namespace: ::prost::alloc::string::String,
    #[prost(enumeration="ConsistencyLevel",tag="5")] pub consistency: i32,
    #[prost(bool,tag="6")] pub reverse: bool,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct ScanResponse {
    #[prost(bytes="vec",tag="1")] pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec",tag="2")] pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64,tag="3")] pub timestamp: u64,
}
#[derive(Clone,PartialEq,::prost::Message)] pub struct ClusterStatusRequest {}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct ClusterStatusResponse {
    #[prost(string,tag="1")] pub cluster_id: ::prost::alloc::string::String,
    #[prost(uint32,tag="2")] pub node_count: u32,
    #[prost(message,repeated,tag="3")] pub nodes: ::prost::alloc::vec::Vec<NodeStatus>,
    #[prost(string,tag="4")] pub leader_id: ::prost::alloc::string::String,
    #[prost(uint64,tag="5")] pub current_term: u64,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct NodeStatus {
    #[prost(string,tag="1")] pub node_id: ::prost::alloc::string::String,
    #[prost(string,tag="2")] pub address: ::prost::alloc::string::String,
    #[prost(enumeration="NodeState",tag="3")] pub state: i32,
    #[prost(message,optional,tag="4")] pub storage: ::core::option::Option<StorageInfo>,
    #[prost(uint64,tag="5")] pub uptime_seconds: u64,
    #[prost(double,tag="6")] pub cpu_usage: f64,
    #[prost(uint64,tag="7")] pub memory_bytes: u64,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct StorageInfo {
    #[prost(uint64,tag="1")] pub ssd_total_bytes: u64,
    #[prost(uint64,tag="2")] pub ssd_used_bytes: u64,
    #[prost(uint64,tag="3")] pub hdd_total_bytes: u64,
    #[prost(uint64,tag="4")] pub hdd_used_bytes: u64,
    #[prost(uint64,tag="5")] pub s3_used_bytes: u64,
    #[prost(string,tag="6")] pub storage_mode: ::prost::alloc::string::String,
    #[prost(uint32,tag="7")] pub sstable_count: u32,
    #[prost(uint32,tag="8")] pub level_count: u32,
}
#[derive(Clone,PartialEq,::prost::Message)] pub struct NodeInfoRequest {}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct NodeInfoResponse {
    #[prost(string,tag="1")] pub node_id: ::prost::alloc::string::String,
    #[prost(string,tag="2")] pub version: ::prost::alloc::string::String,
    #[prost(string,tag="3")] pub address: ::prost::alloc::string::String,
    #[prost(enumeration="NodeState",tag="4")] pub state: i32,
    #[prost(message,optional,tag="5")] pub storage: ::core::option::Option<StorageInfo>,
    #[prost(map="string,string",tag="6")] pub config: ::std::collections::HashMap<::prost::alloc::string::String,::prost::alloc::string::String>,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct JoinClusterRequest {
    #[prost(string,tag="1")] pub node_id: ::prost::alloc::string::String,
    #[prost(string,tag="2")] pub address: ::prost::alloc::string::String,
}
#[derive(Clone,PartialEq,::prost::Message)]
pub struct JoinClusterResponse {
    #[prost(bool,tag="1")] pub success: bool,
    #[prost(string,tag="2")] pub cluster_id: ::prost::alloc::string::String,
    #[prost(string,repeated,tag="3")] pub peer_addresses: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}

pub mod plexus_db_server {
    use super::*;
    #[tonic::async_trait]
    pub trait PlexusDb: Send + Sync + 'static {
        async fn put(&self, request: tonic::Request<PutRequest>) -> Result<tonic::Response<PutResponse>, tonic::Status>;
        async fn get(&self, request: tonic::Request<GetRequest>) -> Result<tonic::Response<GetResponse>, tonic::Status>;
        async fn delete(&self, request: tonic::Request<DeleteRequest>) -> Result<tonic::Response<DeleteResponse>, tonic::Status>;
        async fn batch_put(&self, request: tonic::Request<BatchPutRequest>) -> Result<tonic::Response<BatchPutResponse>, tonic::Status>;
        async fn batch_get(&self, request: tonic::Request<BatchGetRequest>) -> Result<tonic::Response<BatchGetResponse>, tonic::Status>;
        async fn batch_delete(&self, request: tonic::Request<BatchDeleteRequest>) -> Result<tonic::Response<BatchDeleteResponse>, tonic::Status>;
        type ScanStream: tonic::codegen::tokio_stream::Stream<Item=Result<ScanResponse,tonic::Status>> + Send + 'static;
        async fn scan(&self, request: tonic::Request<ScanRequest>) -> Result<tonic::Response<Self::ScanStream>, tonic::Status>;
        async fn cluster_status(&self, request: tonic::Request<ClusterStatusRequest>) -> Result<tonic::Response<ClusterStatusResponse>, tonic::Status>;
        async fn node_info(&self, request: tonic::Request<NodeInfoRequest>) -> Result<tonic::Response<NodeInfoResponse>, tonic::Status>;
        async fn join_cluster(&self, request: tonic::Request<JoinClusterRequest>) -> Result<tonic::Response<JoinClusterResponse>, tonic::Status>;
    }
    pub struct PlexusDbServer<T: PlexusDb> { inner: ::std::sync::Arc<T> }
    impl<T: PlexusDb> PlexusDbServer<T> {
        pub fn new(inner: T) -> Self { Self { inner: ::std::sync::Arc::new(inner) } }
    }
    impl<T: PlexusDb> Clone for PlexusDbServer<T> {
        fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
    }
    impl<T: PlexusDb> tonic::server::NamedService for PlexusDbServer<T> {
        const NAME: &'static str = "plexus.v1.PlexusDB";
    }
    impl<T,B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for PlexusDbServer<T>
    where T: PlexusDb, B: tonic::codegen::Body + Send + 'static, B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    {
        type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(),Self::Error>> { std::task::Poll::Ready(Ok(())) }
        fn call(&mut self, req: tonic::codegen::http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/plexus.v1.PlexusDB/Put" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<PutRequest> for S<T> {
                        type Response=PutResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<PutRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.put(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/Get" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<GetRequest> for S<T> {
                        type Response=GetResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<GetRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.get(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/Delete" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<DeleteRequest> for S<T> {
                        type Response=DeleteResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<DeleteRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.delete(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/BatchPut" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<BatchPutRequest> for S<T> {
                        type Response=BatchPutResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<BatchPutRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.batch_put(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/BatchGet" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<BatchGetRequest> for S<T> {
                        type Response=BatchGetResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<BatchGetRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.batch_get(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/BatchDelete" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<BatchDeleteRequest> for S<T> {
                        type Response=BatchDeleteResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<BatchDeleteRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.batch_delete(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/Scan" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::ServerStreamingService<ScanRequest> for S<T> {
                        type Response=ScanResponse; type ResponseStream=T::ScanStream;
                        type Future=tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<ScanRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.scan(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.server_streaming(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/ClusterStatus" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<ClusterStatusRequest> for S<T> {
                        type Response=ClusterStatusResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<ClusterStatusRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.cluster_status(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/NodeInfo" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<NodeInfoRequest> for S<T> {
                        type Response=NodeInfoResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<NodeInfoRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.node_info(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                "/plexus.v1.PlexusDB/JoinCluster" => {
                    struct S<T:PlexusDb>(::std::sync::Arc<T>);
                    impl<T:PlexusDb> tonic::server::UnaryService<JoinClusterRequest> for S<T> {
                        type Response=JoinClusterResponse; type Future=tonic::codegen::BoxFuture<tonic::Response<Self::Response>,tonic::Status>;
                        fn call(&mut self,req:tonic::Request<JoinClusterRequest>)->Self::Future{let i=self.0.clone();Box::pin(async move{i.join_cluster(req).await})}
                    }
                    let mut g=tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Box::pin(async move{Ok(g.unary(S(inner),req).await)})
                }
                _ => Box::pin(async move{Ok(tonic::codegen::http::Response::builder().status(404).body(tonic::body::empty_body()).unwrap())}),
            }
        }
    }
}

pub mod plexus_db_client {
    use super::*;
    #[derive(Debug,Clone)]
    pub struct PlexusDbClient<T> { inner: tonic::client::Grpc<T> }
    impl PlexusDbClient<tonic::transport::Channel> {
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where D: TryInto<tonic::transport::Endpoint>, D::Error: Into<tonic::codegen::StdError> {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> PlexusDbClient<T>
    where T: tonic::client::GrpcService<tonic::body::BoxBody>, T::Error: Into<tonic::codegen::StdError>,
          T::ResponseBody: tonic::codegen::Body<Data=tonic::codegen::Bytes>+Send+'static,
          <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError>+Send,
    {
        pub fn new(inner: T) -> Self { Self { inner: tonic::client::Grpc::new(inner) } }
        pub async fn put(&mut self, request: impl tonic::IntoRequest<PutRequest>) -> Result<tonic::Response<PutResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/Put"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn get(&mut self, request: impl tonic::IntoRequest<GetRequest>) -> Result<tonic::Response<GetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/Get"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn delete(&mut self, request: impl tonic::IntoRequest<DeleteRequest>) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/Delete"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn batch_put(&mut self, request: impl tonic::IntoRequest<BatchPutRequest>) -> Result<tonic::Response<BatchPutResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/BatchPut"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn batch_get(&mut self, request: impl tonic::IntoRequest<BatchGetRequest>) -> Result<tonic::Response<BatchGetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/BatchGet"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn batch_delete(&mut self, request: impl tonic::IntoRequest<BatchDeleteRequest>) -> Result<tonic::Response<BatchDeleteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/BatchDelete"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn scan(&mut self, request: impl tonic::IntoRequest<ScanRequest>) -> Result<tonic::Response<tonic::codec::Streaming<ScanResponse>>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.server_streaming(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/Scan"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn node_info(&mut self, request: impl tonic::IntoRequest<NodeInfoRequest>) -> Result<tonic::Response<NodeInfoResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/NodeInfo"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn cluster_status(&mut self, request: impl tonic::IntoRequest<ClusterStatusRequest>) -> Result<tonic::Response<ClusterStatusResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/ClusterStatus"), tonic::codec::ProstCodec::default()).await
        }
        pub async fn join_cluster(&mut self, request: impl tonic::IntoRequest<JoinClusterRequest>) -> Result<tonic::Response<JoinClusterResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|_| tonic::Status::new(tonic::Code::Unknown, "service not ready"))?;
            self.inner.unary(request.into_request(), tonic::codegen::http::uri::PathAndQuery::from_static("/plexus.v1.PlexusDB/JoinCluster"), tonic::codec::ProstCodec::default()).await
        }
    }
}



