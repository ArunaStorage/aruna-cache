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
            Resource::Project(i) => *i,
            Resource::Collection(i) => *i,
            Resource::Dataset(i) => *i,
            Resource::Object(i) => *i,
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum ResourcePermission {
    Resource,
    GlobalAdmin,
    ServiceAccount,
}
