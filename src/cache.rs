use ahash::RandomState;
use dashmap::{DashMap, DashSet};
use diesel_ulid::DieselUlid;
use aruna_rust_api::api::storage::models::v2::{generic_resource::Resource as ApiResource, PermissionLevel};
use crate::structs::{ResourcePermission, Resource};
use anyhow::Result;
use anyhow::anyhow;


#[derive(Debug)]
pub struct Cache {
    // Graph cache contains From -> [all] 
    pub graph_cache: DashMap<Resource, DashSet<Resource, RandomState>, RandomState>,
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

    pub fn traverse_graph(&self, from: Resource) -> Result<Vec<(Resource, Resource)>> {
        let mut return_vec = Vec::new();
        let l1 = self.graph_cache.get(&from).ok_or_else(|| anyhow::anyhow!("Cannot find resource"))?;
        for l1_item in l1.iter() {
            let l1_cloned = l1_item.clone();
            if let Some(l2) = self.graph_cache.get(&l1_cloned) {
                for l2_item in l2.iter() {
                    let l2_cloned = l2_item.clone();
                    if let Some(l3) = self.graph_cache.get(&l2_cloned) {
                        for l3_item in l3.iter() {
                            let l3_cloned = l3_item.clone();
                            return_vec.push((l2_cloned.clone(), l3_cloned))
                        }
                    }
                    return_vec.push((l1_cloned.clone(), l2_cloned))
                }
            }
            return_vec.push((from.clone(), l1_cloned))
        }
        Ok(return_vec)
    }

    // Gets a list of parent -> child connections, always from parent to child
    pub fn get_parents(&self, from: Resource) -> Result<Vec<(Resource, Resource)>> {
        match &from {
            &Resource::Project(_) => return Err(anyhow!("Project does not have a parent")),
            &Resource::Collection(_) => {
                for ref_val in self.graph_cache.iter() {
                    if ref_val.value().contains(&from) {
                        return Ok(vec![(ref_val.key().clone(), from)])
                    }
                }
                return Err(anyhow!("Cannot find from resource: {:#?}", from))
            },
            &Resource::Dataset(_) => {
                for ref1_val in self.graph_cache.iter() {
                    if ref1_val.value().contains(&from) {
                        for ref2_val in self.graph_cache.iter() {
                            if ref2_val.value().contains(&ref1_val.key()) {
                                return Ok(vec![(ref2_val.key().clone(), ref1_val.key().clone()), (ref1_val.key().clone(), from)])
                            }
                        }
                        return Err(anyhow!("Unable to find link from collection: {:#?} to a project", &ref1_val.key()))
                    }
                }
                return Err(anyhow!("Cannot find from resource: {:#?}", from))},
            &Resource::Object(_) => {
                for ref1_val in self.graph_cache.iter() {
                    if ref1_val.value().contains(&from) {
                        for ref2_val in self.graph_cache.iter() {
                            if ref2_val.value().contains(&ref1_val.key()) {
                                for ref3_val in self.graph_cache.iter() {
                                    if ref3_val.value().contains(&ref2_val.key()) {
                                        return Ok(vec![(ref3_val.key().clone(), ref2_val.key().clone()), (ref2_val.key().clone(), ref1_val.key().clone()), (ref1_val.key().clone(), from)])
                                    }
                                }
                                return Err(anyhow!("Unable to find link from collection: {:#?} to a project", &ref2_val.key()))
                            }
                        }
                        return Err(anyhow!("Unable to find link from dataset: {:#?} to a collection", &ref1_val.key()))
                    }
                }
                return Err(anyhow!("Cannot find from resource: {:#?}", from))
            },
        }
    }

    pub fn add_link(&self, from: Resource, to: Resource) -> Result<()>{
        match (&from, &to) {
            (&Resource::Project(_), &Resource::Collection(_)) => (),
            (&Resource::Project(_), &Resource::Dataset(_)) => (),
            (&Resource::Project(_), &Resource::Object(_)) => (),
            (&Resource::Collection(_), &Resource::Dataset(_)) => (),
            (&Resource::Collection(_), &Resource::Object(_)) => (),
            (&Resource::Dataset(_), &Resource::Object(_)) => (),
            (_, _) => {return Err(anyhow!("Invalid pair from: {:#?}, to: {:#?}", from, to))}
        }
        let entry = self.graph_cache.entry(from).or_default();
        entry.insert(to);
        Ok(())
    }

    pub fn remove_link(&self, from: Resource, to: Resource) {
        let entry = self.graph_cache.entry(from).or_default();
        entry.remove(&to);
    }

    // Shared id cache functions !

    // Exchanges Shared -> Persistent or vice-versa
    pub fn get_associated_id(&self, input: DieselUlid) -> Option<DieselUlid> {
        self.shared_id_cache.get(&input).map(|e| e.clone())
    }

    pub fn add_shared(&self, shared: DieselUlid, persistent: DieselUlid) {
        self.shared_id_cache.insert(shared, persistent);
        self.shared_id_cache.insert(persistent, shared);
    }
    
    pub fn update_shared(&self, shared: DieselUlid, new_persistent: DieselUlid) {
        let old = self.shared_id_cache.insert(shared, new_persistent);
        if let Some(o) = old {
            self.shared_id_cache.remove(&o);
        }
        self.shared_id_cache.insert(new_persistent, shared);
    }
}


#[cfg(test)]
mod tests {
    use diesel_ulid::DieselUlid;

    use super::Cache;

    #[test]
    fn test_shared () {
        let cache = Cache::new();

        let shared_1 = DieselUlid::generate();
        let persistent_1 = DieselUlid::generate();
        let persistent_2 = DieselUlid::generate();

        cache.add_shared(shared_1, persistent_1);

        assert_eq!(cache.get_associated_id(shared_1).unwrap(), persistent_1);
        assert_eq!(cache.get_associated_id(persistent_1).unwrap(), shared_1);
        assert_eq!(cache.shared_id_cache.len(), 2);

        cache.update_shared(shared_1, persistent_2);
        assert_eq!(cache.get_associated_id(shared_1).unwrap(), persistent_2);
        assert_eq!(cache.get_associated_id(persistent_2).unwrap(), shared_1);
        assert_eq!(cache.shared_id_cache.len(), 2);
    }

}