use crate::checksum::{checksum_resource, checksum_user};
use crate::persistence::PersistenceHandler;
use crate::structs::Resource;
use crate::utils::ClientInterceptor;
use anyhow::Result;
use anyhow::{anyhow, bail};
use aruna_rust_api::api::storage::models::v2::{generic_resource, User};
use aruna_rust_api::api::storage::services::v2::collection_service_client::{
    self, CollectionServiceClient,
};
use aruna_rust_api::api::storage::services::v2::dataset_service_client::{
    self, DatasetServiceClient,
};
use aruna_rust_api::api::storage::services::v2::endpoint_service_client::{
    self, EndpointServiceClient,
};
use aruna_rust_api::api::storage::services::v2::object_service_client::{
    self, ObjectServiceClient,
};
use aruna_rust_api::api::storage::services::v2::project_service_client::{
    self, ProjectServiceClient,
};
use aruna_rust_api::api::storage::services::v2::storage_status_service_client::{
    self, StorageStatusServiceClient,
};
use aruna_rust_api::api::storage::services::v2::user_service_client::{self, UserServiceClient};
use aruna_rust_api::api::storage::services::v2::{
    FullSyncEndpointRequest, GetCollectionRequest, GetDatasetRequest, GetObjectRequest,
    GetProjectRequest, GetPubkeysRequest, GetUserRedactedRequest, Pubkey as APIPubkey,
};
use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSyncData {
    pub resources: Vec<(DieselUlid, Resource, generic_resource::Resource)>,
    pub users: Vec<(DieselUlid, User)>,
    pub pubkeys: Vec<APIPubkey>,
}

#[async_trait]
pub trait QueryHandler {
    async fn get_user(&self, id: DieselUlid, checksum: String) -> Result<User>;
    async fn get_pubkeys(&self) -> Result<Vec<APIPubkey>>;
    async fn get_resource(
        &self,
        res: &Resource,
        checksum: String,
    ) -> Result<generic_resource::Resource>;
    async fn full_sync(&self) -> Result<FullSyncData>;
}

pub struct ApiQueryHandler {
    project_service: ProjectServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    collection_service: CollectionServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    dataset_service: DatasetServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    object_service: ObjectServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    user_service: UserServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    endpoint_service: EndpointServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    storage_status_service:
        StorageStatusServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    _persistence: Option<Box<dyn PersistenceHandler + Send + Sync>>,
}

impl ApiQueryHandler {
    async fn new(
        token: impl Into<String>,
        server: impl Into<String>,
        _persistence: Option<Box<dyn PersistenceHandler + Send + Sync>>,
    ) -> Result<Self> {
        let tls_config = ClientTlsConfig::new();
        let endpoint = Channel::from_shared(server.into())?.tls_config(tls_config)?;
        let channel = endpoint.connect().await?;
        let interceptor = ClientInterceptor {
            api_token: token.into(),
        };

        let project_service = project_service_client::ProjectServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let collection_service =
            collection_service_client::CollectionServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            );

        let dataset_service = dataset_service_client::DatasetServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let object_service = object_service_client::ObjectServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let user_service = user_service_client::UserServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let endpoint_service = endpoint_service_client::EndpointServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let storage_status_service =
            storage_status_service_client::StorageStatusServiceClient::with_interceptor(
                channel,
                interceptor,
            );
        Ok(ApiQueryHandler {
            project_service,
            collection_service,
            dataset_service,
            object_service,
            user_service,
            endpoint_service,
            storage_status_service,
            _persistence,
        })
    }
}

#[async_trait]
impl QueryHandler for ApiQueryHandler {
    async fn get_user(&self, id: DieselUlid, checksum: String) -> Result<User> {
        let user = self
            .user_service
            .clone()
            .get_user_redacted(Request::new(GetUserRedactedRequest {
                user_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .user
            .ok_or(anyhow!("Unknown user"))?;

        let actual_checksum = checksum_user(&user)?;

        if actual_checksum == checksum {
            bail!("Invalid checksum")
        }

        Ok(user)
    }
    async fn get_pubkeys(&self) -> Result<Vec<APIPubkey>> {
        Ok(self
            .storage_status_service
            .clone()
            .get_pubkeys(Request::new(GetPubkeysRequest {}))
            .await?
            .into_inner()
            .pubkeys)
    }
    async fn get_resource(
        &self,
        res: &Resource,
        checksum: String,
    ) -> Result<generic_resource::Resource> {
        let resource = match res {
            Resource::Project(id) => generic_resource::Resource::Project(
                self.project_service
                    .clone()
                    .get_project(Request::new(GetProjectRequest {
                        project_id: id.to_string(),
                    }))
                    .await?
                    .into_inner()
                    .project
                    .ok_or(anyhow!("unknown project"))?,
            ),
            Resource::Collection(id) => generic_resource::Resource::Collection(
                self.collection_service
                    .clone()
                    .get_collection(Request::new(GetCollectionRequest {
                        collection_id: id.to_string(),
                    }))
                    .await?
                    .into_inner()
                    .collection
                    .ok_or(anyhow!("unknown collection"))?,
            ),
            Resource::Dataset(id) => generic_resource::Resource::Dataset(
                self.dataset_service
                    .clone()
                    .get_dataset(Request::new(GetDatasetRequest {
                        dataset_id: id.to_string(),
                    }))
                    .await?
                    .into_inner()
                    .dataset
                    .ok_or(anyhow!("unknown dataset"))?,
            ),
            Resource::Object(id) => generic_resource::Resource::Object(
                self.object_service
                    .clone()
                    .get_object(Request::new(GetObjectRequest {
                        object_id: id.to_string(),
                    }))
                    .await?
                    .into_inner()
                    .object
                    .ok_or(anyhow!("unknown object"))?,
            ),
        };

        if checksum != checksum_resource(resource.clone())? {
            bail!("Invalid checksum")
        }

        return Ok(resource);
    }

    async fn full_sync(&self) -> Result<FullSyncData> {
        let result = self
            .endpoint_service
            .clone()
            .full_sync_endpoint(Request::new(FullSyncEndpointRequest {}))
            .await?
            .into_inner()
            .url;
        Ok(reqwest::get(result).await?.json::<FullSyncData>().await?)
    }
}
