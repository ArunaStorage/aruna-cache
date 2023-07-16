use crate::structs::PubKey;
use crate::structs::{Resource, ResourcePermission};
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::{
    generic_resource::Resource as ApiResource, PermissionLevel,
};
use dashmap::{DashMap, DashSet};
use diesel_ulid::DieselUlid;

#[derive(Debug)]
pub struct Cache {
    // Graph cache contains From -> [all]
    pub graph_cache: DashMap<Resource, DashSet<Resource, RandomState>, RandomState>,
    pub name_cache: DashMap<String, DashSet<Resource, RandomState>, RandomState>,
    pub shared_id_cache: DashMap<DieselUlid, DieselUlid, RandomState>,
    pub object_cache: Option<DashMap<DieselUlid, ApiResource, RandomState>>,
    pub permissions:
        DashMap<DieselUlid, DashMap<ResourcePermission, PermissionLevel, RandomState>, RandomState>,
    pub pubkeys: DashSet<PubKey, RandomState>,
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

impl Cache {
    pub fn new() -> Self {
        Cache {
            graph_cache: DashMap::with_hasher(RandomState::new()),
            name_cache: DashMap::with_hasher(RandomState::new()),
            shared_id_cache: DashMap::with_hasher(RandomState::new()),
            object_cache: None,
            permissions: DashMap::with_hasher(RandomState::new()),
            pubkeys: DashSet::with_hasher(RandomState::new()),
        }
    }

    pub fn traverse_graph(&self, from: &Resource) -> Result<Vec<(Resource, Resource)>> {
        let mut return_vec = Vec::new();
        let l1 = self
            .graph_cache
            .get(from)
            .ok_or_else(|| anyhow::anyhow!("Cannot find resource"))?;
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
        return_vec.reverse();
        Ok(return_vec)
    }

    // Gets a list of parent -> child connections, always from parent to child
    pub fn get_parents(&self, from: &Resource) -> Result<Vec<(Resource, Resource)>> {
        let mut return_vec = Vec::new();
        match &from {
            Resource::Project(_) => return Err(anyhow!("Project does not have a parent")),
            Resource::Collection(_) => {
                for ref_val in self.graph_cache.iter() {
                    if ref_val.value().contains(from) {
                        return_vec.push((ref_val.key().clone(), from.clone()));
                        break;
                    }
                }
                if return_vec.is_empty() {
                    return Err(anyhow!("Cannot find from resource: {:#?}", &from));
                }
            }
            Resource::Dataset(_) => {
                for ref1_val in self.graph_cache.iter() {
                    if ref1_val.value().contains(from) {
                        for ref2_val in self.graph_cache.iter() {
                            if ref2_val.value().contains(ref1_val.key()) {
                                return_vec.push((ref2_val.key().clone(), ref1_val.key().clone()));
                                break;
                            }
                        }
                        return_vec.push((ref1_val.key().clone(), from.clone()));
                        break;
                    }
                }
                if return_vec.is_empty() {
                    return Err(anyhow!("Cannot find from resource: {:#?}", &from));
                }
            }
            Resource::Object(_) => {
                for ref1_val in self.graph_cache.iter() {
                    if ref1_val.value().contains(from) {
                        for ref2_val in self.graph_cache.iter() {
                            if ref2_val.value().contains(ref1_val.key()) {
                                for ref3_val in self.graph_cache.iter() {
                                    if ref3_val.value().contains(ref2_val.key()) {
                                        return_vec
                                            .push((ref3_val.key().clone(), ref2_val.key().clone()));
                                        break;
                                    }
                                }
                                return_vec.push((ref2_val.key().clone(), ref1_val.key().clone()));
                                break;
                            }
                        }
                        return_vec.push((ref1_val.key().clone(), from.clone()));
                        break;
                    }
                }

                if return_vec.is_empty() {
                    return Err(anyhow!("Cannot find from resource: {:#?}", &from));
                }
            }
        }
        Ok(return_vec)
    }

    pub fn add_name(&self, res: Resource, name: String) {
        self.name_cache.entry(name).or_default().insert(res);
    }

    pub fn remove_name(&self, res: Resource, name: Option<String>) {
        if let Some(name) = name {
            self.name_cache.entry(name).or_default().remove(&res);
        } else {
            for ent in self.name_cache.iter() {
                if ent.value().contains(&res) {
                    ent.value().remove(&res);
                }
            }
        }
    }

    pub fn add_link(&self, from: Resource, to: Resource) -> Result<()> {
        match (&from, &to) {
            (&Resource::Project(_), &Resource::Collection(_)) => (),
            (&Resource::Project(_), &Resource::Dataset(_)) => (),
            (&Resource::Project(_), &Resource::Object(_)) => (),
            (&Resource::Collection(_), &Resource::Dataset(_)) => (),
            (&Resource::Collection(_), &Resource::Object(_)) => (),
            (&Resource::Dataset(_), &Resource::Object(_)) => (),
            (_, _) => return Err(anyhow!("Invalid pair from: {:#?}, to: {:#?}", from, to)),
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
    pub fn get_associated_id(&self, input: &DieselUlid) -> Option<DieselUlid> {
        self.shared_id_cache.get(input).map(|e| *e)
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

    pub fn add_or_update_permission(
        &self,
        id: DieselUlid,
        perm: (ResourcePermission, PermissionLevel),
    ) {
        let entry = self.permissions.entry(id).or_default();
        entry.insert(perm.0, perm.1);
    }

    pub fn remove_permission(
        &self,
        id: DieselUlid,
        res: Option<ResourcePermission>,
        full_entry: bool,
    ) {
        if full_entry {
            self.permissions.remove(&id);
        } else if let Some(e) = self.permissions.get_mut(&id) {
            if let Some(p) = res {
                e.remove(&p);
            }
        }
    }

    pub fn get_permissions(
        &self,
        id: &DieselUlid,
    ) -> Option<Vec<(ResourcePermission, PermissionLevel)>> {
        let perms = self.permissions.get(id)?;
        let mut return_vec = Vec::new();
        for x in perms.value() {
            return_vec.push((x.key().clone(), *x.value()))
        }
        Some(return_vec)
    }

    pub fn add_pubkey(&self, pk: PubKey) {
        self.pubkeys.insert(pk);
    }

    pub fn remove_pubkey(&self, pk: PubKey) {
        self.pubkeys.remove(&pk);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::Resource::*;

    #[test]
    fn test_shared() {
        let cache = Cache::new();

        let shared_1 = DieselUlid::generate();
        let persistent_1 = DieselUlid::generate();
        let persistent_2 = DieselUlid::generate();

        cache.add_shared(shared_1, persistent_1);

        assert_eq!(cache.get_associated_id(&shared_1).unwrap(), persistent_1);
        assert_eq!(cache.get_associated_id(&persistent_1).unwrap(), shared_1);
        assert_eq!(cache.shared_id_cache.len(), 2);

        cache.update_shared(shared_1, persistent_2);
        assert_eq!(cache.get_associated_id(&shared_1).unwrap(), persistent_2);
        assert_eq!(cache.get_associated_id(&persistent_2).unwrap(), shared_1);
        assert_eq!(cache.shared_id_cache.len(), 2);
    }

    #[test]
    fn test_graph() {
        let cache = Cache::new();

        let project_1 = Project(DieselUlid::generate());
        let collection_1 = Collection(DieselUlid::generate());
        let _collection_2 = Collection(DieselUlid::generate());
        let _dataset_1 = Dataset(DieselUlid::generate());
        let _dataset_2 = Dataset(DieselUlid::generate());
        let _dataset_3 = Dataset(DieselUlid::generate());
        let _object_1 = Object(DieselUlid::generate());
        let _object_2 = Object(DieselUlid::generate());
        let _object_3 = Object(DieselUlid::generate());
        let _object_4 = Object(DieselUlid::generate());

        cache
            .add_link(project_1.clone(), collection_1.clone())
            .unwrap();
        assert_eq!(
            cache.get_parents(&collection_1).unwrap(),
            vec![(project_1.clone(), collection_1.clone())]
        );
        assert_eq!(
            cache.traverse_graph(&project_1).unwrap(),
            vec![(project_1.clone(), collection_1.clone())]
        );
        cache
            .add_link(project_1.clone(), collection_1.clone())
            .unwrap();
    }

    #[test]
    fn test_traverse_graph() {
        let cache = Cache::new();
        let resource_a = Resource::Project(DieselUlid::generate());
        let resource_b = Resource::Collection(DieselUlid::generate());
        let resource_c = Resource::Dataset(DieselUlid::generate());
        let resource_d = Resource::Object(DieselUlid::generate());

        // Add entries to the graph cache
        cache
            .add_link(resource_a.clone(), resource_b.clone())
            .unwrap();
        cache
            .add_link(resource_b.clone(), resource_c.clone())
            .unwrap();
        cache
            .add_link(resource_c.clone(), resource_d.clone())
            .unwrap();

        // Test the traverse_graph function
        let result = cache.traverse_graph(&resource_a).unwrap();
        let expected = vec![
            (resource_a.clone(), resource_b.clone()),
            (resource_b.clone(), resource_c.clone()),
            (resource_c.clone(), resource_d.clone()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_get_parents() {
        let cache = Cache::new();
        let resource_a = Resource::Project(DieselUlid::generate());
        let resource_b = Resource::Collection(DieselUlid::generate());
        let resource_c = Resource::Dataset(DieselUlid::generate());
        let resource_d = Resource::Object(DieselUlid::generate());

        // Add entries to the graph cache
        cache
            .add_link(resource_a.clone(), resource_b.clone())
            .unwrap();
        cache
            .add_link(resource_b.clone(), resource_c.clone())
            .unwrap();
        cache
            .add_link(resource_c.clone(), resource_d.clone())
            .unwrap();

        // Test the get_parents function
        let result = cache.get_parents(&resource_d).unwrap();
        let expected = vec![
            (resource_a.clone(), resource_b.clone()),
            (resource_b.clone(), resource_c.clone()),
            (resource_c.clone(), resource_d.clone()),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_add_name() {
        let cache = Cache::new();
        let resource_a = Resource::Project(DieselUlid::generate());
        let name = "NameA".to_owned();

        // Test adding a name
        cache.add_name(resource_a.clone(), name.clone());

        // Check if the name is present in the cache
        let result = cache
            .name_cache
            .get(&name)
            .map(|entry| entry.clone().into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let expected = vec![resource_a.clone()];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_remove_name() {
        let cache = Cache::new();
        let resource_a = Resource::Project(DieselUlid::generate());
        let resource_b = Resource::Project(DieselUlid::generate());
        let name = "NameA".to_owned();

        // Add names to the cache
        cache.add_name(resource_a.clone(), name.clone());
        cache.add_name(resource_b.clone(), name.clone());

        // Test removing a specific name
        cache.remove_name(resource_a.clone(), Some(name.clone()));

        // Check if the name is removed for the specific resource
        let result = cache
            .name_cache
            .get(&name)
            .map(|entry| entry.clone().into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let expected = vec![resource_b.clone()];
        assert_eq!(result, expected);

        // Test removing all names associated with a resource
        cache.remove_name(resource_b.clone(), Some(name.clone()));

        // Check if all names associated with the resource are removed
        assert!(cache.name_cache.get(&name).unwrap().is_empty());
    }

    #[test]
    fn test_add_link() {
        let cache = Cache::new();
        let resource_a = Resource::Project(DieselUlid::generate());
        let resource_b = Resource::Collection(DieselUlid::generate());

        // Test adding a valid link
        let result = cache.add_link(resource_a.clone(), resource_b.clone());
        assert!(result.is_ok());

        // Test adding an invalid link
        let resource_c = Resource::Dataset(DieselUlid::generate());
        let result = cache.add_link(resource_c.clone(), resource_a.clone());
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_link() {
        let cache = Cache::new();
        let resource_a = Resource::Project(DieselUlid::generate());
        let resource_b = Resource::Collection(DieselUlid::generate());

        // Add a link to the cache
        cache
            .add_link(resource_a.clone(), resource_b.clone())
            .unwrap();

        // Test removing the link
        cache.remove_link(resource_a.clone(), resource_b.clone());

        // Check if the link is removed
        assert!(cache.graph_cache.get(&resource_a).unwrap().is_empty());
    }

    #[test]
    fn test_get_associated_id() {
        let cache = Cache::new();
        let shared_id = DieselUlid::generate();
        let persistent_id = DieselUlid::generate();

        // Add an entry to the shared id cache
        cache.add_shared(shared_id.clone(), persistent_id.clone());

        // Test getting the associated persistent id
        let result = cache.get_associated_id(&shared_id);
        assert_eq!(result, Some(persistent_id));

        // Test getting the associated shared id
        let result = cache.get_associated_id(&persistent_id);
        assert_eq!(result, Some(shared_id));

        // Test getting an associated id that doesn't exist
        let invalid_id = DieselUlid::generate();
        let result = cache.get_associated_id(&invalid_id);
        assert_eq!(result, None);
    }

    #[test]
    fn test_add_or_update_permission() {
        let cache = Cache::new();
        let resource_id = DieselUlid::generate();
        let permission = (
            ResourcePermission::Resource(Resource::Project(DieselUlid::generate())),
            PermissionLevel::Read,
        );

        // Test adding a permission
        cache.add_or_update_permission(resource_id.clone(), permission.clone());

        // Check if the permission is added
        let result = cache
            .permissions
            .get(&resource_id)
            .map(|entry| entry.clone().into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let expected = vec![(permission.clone().0, permission.clone().1)];
        assert_eq!(result, expected);

        // Test updating a permission
        let updated_permission = (ResourcePermission::GlobalAdmin, PermissionLevel::Write);
        cache.add_or_update_permission(resource_id.clone(), updated_permission.clone());

        // Check if the permission is updated
        let result = cache
            .permissions
            .get(&resource_id)
            .map(|entry| entry.clone().into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let expected = vec![permission, (updated_permission.0, updated_permission.1)];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_remove_permission() {
        let cache = Cache::new();
        let resource_id = DieselUlid::generate();
        let permission = (
            ResourcePermission::Resource(Resource::Project(DieselUlid::generate())),
            PermissionLevel::Read,
        );

        // Add a permission to the cache
        cache.add_or_update_permission(resource_id.clone(), permission.clone());

        // Test removing a specific permission
        cache.remove_permission(resource_id.clone(), Some(permission.0), true);

        // Check if the permission is removed for the specific resource
        assert!(!cache.permissions.contains_key(&resource_id));
    }

    #[test]
    fn test_get_permissions() {
        let cache = Cache::new();
        let resource_id = DieselUlid::generate();
        let permission_1 = (
            ResourcePermission::Resource(Resource::Project(DieselUlid::generate())),
            PermissionLevel::Read,
        );
        let permission_2 = (ResourcePermission::GlobalAdmin, PermissionLevel::Write);

        // Add permissions to the cache
        cache.add_or_update_permission(resource_id.clone(), permission_1.clone());
        cache.add_or_update_permission(resource_id.clone(), permission_2.clone());

        // Test getting permissions
        let result = cache
            .get_permissions(&resource_id)
            .map(|perms| perms.into_iter().collect::<Vec<_>>())
            .unwrap();
        let expected = vec![permission_1.clone(), permission_2.clone()];
        assert!(result.iter().all(|item| expected.contains(item)));
    }

    #[test]
    fn test_add_pubkey() {
        let cache = Cache::new();
        let pubkey = PubKey::DataProxy("pubkey".to_owned());

        // Test adding a pubkey
        cache.add_pubkey(pubkey.clone());

        // Check if the pubkey is present in the cache
        assert!(cache.pubkeys.contains(&pubkey));
    }

    #[test]
    fn test_remove_pubkey() {
        let cache = Cache::new();
        let pubkey_a = PubKey::DataProxy("pubkey_a".to_owned());
        let pubkey_b = PubKey::DataProxy("pubkey_b".to_owned());

        // Add pubkeys to the cache
        cache.add_pubkey(pubkey_a.clone());
        cache.add_pubkey(pubkey_b.clone());

        // Test removing a pubkey
        cache.remove_pubkey(pubkey_a.clone());

        // Check if the pubkey is removed from the cache
        assert!(!cache.pubkeys.contains(&pubkey_a));
    }

    #[test]
    fn test_update_shared() {
        let cache = Cache::new();
        let shared_id = DieselUlid::generate();
        let new_persistent_id = DieselUlid::generate();

        // Test updating a shared id
        cache.update_shared(shared_id.clone(), new_persistent_id.clone());

        // Check if the shared id is updated
        cache.shared_id_cache.get(&shared_id);

        // Check if the new persistent id is associated with the shared id
        let result = cache.get_associated_id(&shared_id);
        assert_eq!(result, Some(new_persistent_id));
    }
}
