use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone)]
pub enum Resource {
    Project(DieselUlid),
    Collection(DieselUlid),
    Dataset(DieselUlid),
    Object(DieselUlid),
}

impl TryFrom<ResourceId> for Resource {
    type Error = anyhow::Error;

    fn try_from(value: ResourceId) -> Result<Self, Self::Error> {
        match value {
            ResourceId::ProjectId(id) => Ok(Self::Project(DieselUlid::from_str(&id)?)),
            ResourceId::CollectionId(id) => Ok(Self::Collection(DieselUlid::from_str(&id)?)),
            ResourceId::DatasetId(id) => Ok(Self::Dataset(DieselUlid::from_str(&id)?)),
            ResourceId::ObjectId(id) => Ok(Self::Object(DieselUlid::from_str(&id)?)),
        }
    }
}

impl Resource {
    pub fn get_id(&self) -> DieselUlid {
        match self {
            Resource::Project(i) => *i,
            Resource::Collection(i) => *i,
            Resource::Dataset(i) => *i,
            Resource::Object(i) => *i,
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone)]
pub enum ResourcePermission {
    Resource(Resource),
    GlobalAdmin,
    ServiceAccount,
}

impl From<Resource> for ResourcePermission {
    fn from(value: Resource) -> Self {
        ResourcePermission::Resource(value)
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone)]
pub enum PubKey {
    DataProxy(String),
    Server(String),
}