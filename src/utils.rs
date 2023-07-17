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
        let (res_id, associated_id) = if self.persistent_resource_id {
            (
                DieselUlid::from_str(&self.resource_id).ok()?,
                DieselUlid::from_str(&self.associated_id).ok()?,
            )
        } else {
            (
                DieselUlid::from_str(&self.associated_id).ok()?,
                DieselUlid::from_str(&self.resource_id).ok()?,
            )
        };
        let (associated_id, res) = match self.resource_variant() {
            ResourceVariant::Project => (res_id, Resource::Project(associated_id)),
            ResourceVariant::Collection => (res_id, Resource::Collection(associated_id)),
            ResourceVariant::Dataset => (res_id, Resource::Dataset(associated_id)),
            ResourceVariant::Object => (res_id, Resource::Object(associated_id)),
            _ => return None,
        };

        Some((associated_id, res))
    }
}
