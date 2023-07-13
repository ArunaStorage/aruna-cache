use diesel_ulid::DieselUlid;

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone)]
pub enum Resource {
    Project(DieselUlid),
    Collection(DieselUlid),
    Dataset(DieselUlid),
    Object(DieselUlid),
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum ResourcePermission {
    Resource,
    GlobalAdmin,
    ServiceAccount,
}
