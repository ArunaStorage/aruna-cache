use diesel_ulid::DieselUlid;

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone)]
pub enum Resource {
    Project(DieselUlid),
    Collection(DieselUlid),
    Dataset(DieselUlid),
    Object(DieselUlid),
}

impl Resource {
    pub fn get_id(&self) -> DieselUlid {
        match self {
            Resource::Project(i) => i.clone(),
            Resource::Collection(i) => i.clone(),
            Resource::Dataset(i) => i.clone(),
            Resource::Object(i) => i.clone(),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum ResourcePermission {
    Resource,
    GlobalAdmin,
    ServiceAccount,
}
