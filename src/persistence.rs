use anyhow::Result;
use aruna_rust_api::api::storage::{
    models::v2::{generic_resource, User},
    services::v2::Pubkey,
};
use async_trait::async_trait;
use diesel_ulid::DieselUlid;

#[async_trait]
pub trait PersistenceHandler {
    async fn persist_resource(
        &self,
        id: DieselUlid,
        shared_id: DieselUlid,
        object_data: generic_resource::Resource,
    ) -> Result<()>;
    async fn persist_pubkey(&self, pks: Vec<Pubkey>) -> Result<()>;
    async fn persist_user(&self, id: DieselUlid, user: User) -> Result<()>;
    async fn get_resource(&self, id: DieselUlid) -> Result<generic_resource::Resource>;
    async fn get_resources(&self) -> Result<Vec<generic_resource::Resource>>;
    async fn get_user(&self, id: DieselUlid) -> Result<User>;
    async fn get_users(&self) -> Result<Vec<User>>;
}
