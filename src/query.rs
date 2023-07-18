use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::{Collection, Dataset, Object, Project, User};
use aruna_rust_api::api::storage::services::v2::Pubkey as APIPubkey;
use async_trait::async_trait;
use diesel_ulid::DieselUlid;

#[async_trait]
pub trait QueryHandler {
    async fn get_user(&self, id: DieselUlid) -> Result<User>;
    async fn get_pubkeys(&self) -> Result<Vec<APIPubkey>>;
    async fn get_project(&self, id: DieselUlid) -> Result<Project>;
    async fn get_collection(&self, id: DieselUlid) -> Result<Collection>;
    async fn get_dataset(&self, id: DieselUlid) -> Result<Dataset>;
    async fn get_object(&self, id: DieselUlid) -> Result<Object>;
}

// project_service: Option<ProjectServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
// collection_service:
//     Option<CollectionServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
// dataset_service: Option<DatasetServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
// object_service: Option<ObjectServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
// user_service: Option<UserServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
// endpoint_service: Option<EndpointServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
// storage_status_service:
//     Option<StorageStatusServiceClient<InterceptedService<Channel, ClientInterceptor>>>,

// let project_service = project_service_client::ProjectServiceClient::with_interceptor(
//     channel.clone(),
//     interceptor.clone(),
// );

// let collection_service =
//     collection_service_client::CollectionServiceClient::with_interceptor(
//         channel.clone(),
//         interceptor.clone(),
//     );

// let dataset_service = dataset_service_client::DatasetServiceClient::with_interceptor(
//     channel.clone(),
//     interceptor.clone(),
// );

// let object_service = object_service_client::ObjectServiceClient::with_interceptor(
//     channel.clone(),
//     interceptor.clone(),
// );

// let user_service = user_service_client::UserServiceClient::with_interceptor(
//     channel.clone(),
//     interceptor.clone(),
// );

// let endpoint_service = endpoint_service_client::EndpointServiceClient::with_interceptor(
//     channel.clone(),
//     interceptor.clone(),
// );

// let storage_status_service =
//     storage_status_service_client::StorageStatusServiceClient::with_interceptor(
//         channel,
//         interceptor,
//     );
