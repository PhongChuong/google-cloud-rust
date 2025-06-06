// This file is @generated by prost-build.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Operation {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub metadata: ::core::option::Option<::prost_types::Any>,
    #[prost(bool, tag = "3")]
    pub done: bool,
    #[prost(oneof = "operation::Result", tags = "4, 5")]
    pub result: ::core::option::Option<operation::Result>,
}
/// Nested message and enum types in `Operation`.
pub mod operation {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "4")]
        Error(super::super::rpc::Status),
        #[prost(message, tag = "5")]
        Response(::prost_types::Any),
    }
}
impl ::prost::Name for Operation {
    const NAME: &'static str = "Operation";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.Operation".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.Operation".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOperationRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
impl ::prost::Name for GetOperationRequest {
    const NAME: &'static str = "GetOperationRequest";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.GetOperationRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.GetOperationRequest".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOperationsRequest {
    #[prost(string, tag = "4")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "1")]
    pub filter: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub page_size: i32,
    #[prost(string, tag = "3")]
    pub page_token: ::prost::alloc::string::String,
}
impl ::prost::Name for ListOperationsRequest {
    const NAME: &'static str = "ListOperationsRequest";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.ListOperationsRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.ListOperationsRequest".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOperationsResponse {
    #[prost(message, repeated, tag = "1")]
    pub operations: ::prost::alloc::vec::Vec<Operation>,
    #[prost(string, tag = "2")]
    pub next_page_token: ::prost::alloc::string::String,
}
impl ::prost::Name for ListOperationsResponse {
    const NAME: &'static str = "ListOperationsResponse";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.ListOperationsResponse".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.ListOperationsResponse".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelOperationRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
impl ::prost::Name for CancelOperationRequest {
    const NAME: &'static str = "CancelOperationRequest";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.CancelOperationRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.CancelOperationRequest".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteOperationRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
impl ::prost::Name for DeleteOperationRequest {
    const NAME: &'static str = "DeleteOperationRequest";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.DeleteOperationRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.DeleteOperationRequest".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WaitOperationRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub timeout: ::core::option::Option<::prost_types::Duration>,
}
impl ::prost::Name for WaitOperationRequest {
    const NAME: &'static str = "WaitOperationRequest";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.WaitOperationRequest".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.WaitOperationRequest".into()
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OperationInfo {
    #[prost(string, tag = "1")]
    pub response_type: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub metadata_type: ::prost::alloc::string::String,
}
impl ::prost::Name for OperationInfo {
    const NAME: &'static str = "OperationInfo";
    const PACKAGE: &'static str = "google.longrunning";
    fn full_name() -> ::prost::alloc::string::String {
        "google.longrunning.OperationInfo".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.longrunning.OperationInfo".into()
    }
}
