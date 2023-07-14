use crate::structs::Resource;
use aruna_rust_api::api::{
    notification::services::v2::Resource as ApiResource, storage::models::v2::ResourceVariant,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub trait GetRef {
    fn get_ref(&self) -> Option<(DieselUlid, Resource)>;
}

impl GetRef for ApiResource {
    fn get_ref(&self) -> Option<(DieselUlid, Resource)> {
        let (associated_id, res) = match self.resource_variant() {
            ResourceVariant::Project => (
                DieselUlid::from_str(&self.resource_id).ok()?,
                Resource::Project(DieselUlid::from_str(&self.associated_id).ok()?),
            ),
            ResourceVariant::Collection => (
                DieselUlid::from_str(&self.resource_id).ok()?,
                Resource::Collection(DieselUlid::from_str(&self.associated_id).ok()?),
            ),
            ResourceVariant::Dataset => (
                DieselUlid::from_str(&self.resource_id).ok()?,
                Resource::Dataset(DieselUlid::from_str(&self.associated_id).ok()?),
            ),
            ResourceVariant::Object => (
                DieselUlid::from_str(&self.associated_id).ok()?,
                Resource::Dataset(DieselUlid::from_str(&self.resource_id).ok()?),
            ),
            _ => return None,
            ResourceVariant::Unspecified => todo!(),
        };

        Some((associated_id, res))
    }
}
