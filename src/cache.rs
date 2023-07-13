use ahash::RandomState;
use dashmap::{DashMap, DashSet};
use diesel_ulid::DieselUlid;
use aruna_rust_api::api::storage::models::v2::{generic_resource::Resource as ApiResource, PermissionLevel};
use crate::structs::{ResourcePermission, Resource};


#[derive(Debug)]
pub struct Cache {
    // Graph cache contains From -> [all] 
    pub graph_cache: DashMap<DieselUlid, DashSet<Resource, RandomState>, RandomState>,
    pub shared_id_cache: DashMap<DieselUlid, DieselUlid, RandomState>,
    pub object_cache: Option<DashMap<DieselUlid, ApiResource, RandomState>>,
    pub permissions: DashMap<DieselUlid, DashMap<ResourcePermission, PermissionLevel, RandomState>, RandomState>,
}

impl Cache {
    pub fn new() -> Self {
        Cache {
            graph_cache: DashMap::with_hasher(RandomState::new()),
            shared_id_cache: DashMap::with_hasher(RandomState::new()),
            object_cache: None,
            permissions: DashMap::with_hasher(RandomState::new()),
        }
    }
}