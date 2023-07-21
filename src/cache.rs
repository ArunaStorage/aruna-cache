use std::str::FromStr;

use crate::structs::PubKey;
use crate::structs::Resource;
use crate::utils::internal_relation_to_rel;
use ahash::{HashMap, RandomState};
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource as ApiResource;
use aruna_rust_api::api::storage::models::v2::{
    relation, InternalRelationVariant, Relation, ResourceVariant, User,
};
use aruna_rust_api::api::storage::services::v2::Pubkey;
use dashmap::{DashMap, DashSet};
use diesel_ulid::DieselUlid;

#[derive(Debug)]
pub struct Cache {
    pub relations_cache: DashMap<Resource, DashSet<Resource, RandomState>, RandomState>,
    pub name_cache: DashMap<String, DashSet<Resource, RandomState>, RandomState>,
    pub shared_to_pid: DashMap<DieselUlid, Resource, RandomState>,
    pub pid_to_shared: DashMap<DieselUlid, Resource, RandomState>,
    pub object_cache: DashMap<Resource, ApiResource, RandomState>,
    pub user_cache: DashMap<DieselUlid, User, RandomState>,
    pub pubkeys: DashMap<i32, PubKey, RandomState>,
    pub oidc_ids: DashMap<String, DieselUlid, RandomState>,
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

impl Cache {
    pub fn new() -> Self {
        Cache {
            relations_cache: DashMap::with_hasher(RandomState::new()),
            name_cache: DashMap::with_hasher(RandomState::new()),
            shared_to_pid: DashMap::with_hasher(RandomState::new()),
            pid_to_shared: DashMap::with_hasher(RandomState::new()),
            object_cache: DashMap::with_hasher(RandomState::new()),
            user_cache: DashMap::with_hasher(RandomState::new()),
            pubkeys: DashMap::with_hasher(RandomState::new()),
            oidc_ids: DashMap::with_hasher(RandomState::new()),
        }
    }

    pub fn traverse_graph(&self, from: &Resource) -> Result<Vec<(Resource, Resource)>> {
        let mut return_vec = Vec::new();
        let l1 = self
            .relations_cache
            .get(from)
            .ok_or_else(|| anyhow::anyhow!("Cannot find resource"))?;
        for l1_item in l1.iter() {
            let l1_cloned = l1_item.clone();
            if let Some(l2) = self.relations_cache.get(&l1_cloned) {
                for l2_item in l2.iter() {
                    let l2_cloned = l2_item.clone();
                    if let Some(l3) = self.relations_cache.get(&l2_cloned) {
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

    pub fn get_parents_with_targets(&self, from: &Resource, targets: Vec<Resource>) -> Result<()> {
        match &from {
            Resource::Project(_) => return Err(anyhow!("Project does not have a parent")),
            Resource::Collection(_) => {
                for ref_val in self.relations_cache.iter() {
                    if ref_val.value().contains(from) && targets.contains(ref_val.key()) {
                        return Ok(());
                    }
                }
            }
            Resource::Dataset(_) => {
                for ref1_val in self.relations_cache.iter() {
                    if ref1_val.value().contains(from) {
                        if targets.contains(ref1_val.key()) {
                            return Ok(());
                        }
                        for ref2_val in self.relations_cache.iter() {
                            if ref2_val.value().contains(ref1_val.key())
                                && targets.contains(ref2_val.key())
                            {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Resource::Object(_) => {
                for ref1_val in self.relations_cache.iter() {
                    if ref1_val.value().contains(from) {
                        if targets.contains(ref1_val.key()) {
                            return Ok(());
                        }
                        for ref2_val in self.relations_cache.iter() {
                            if ref2_val.value().contains(ref1_val.key()) {
                                if targets.contains(ref2_val.key()) {
                                    return Ok(());
                                }
                                for ref3_val in self.relations_cache.iter() {
                                    if ref3_val.value().contains(ref2_val.key())
                                        && targets.contains(ref3_val.key())
                                    {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Err(anyhow!("Cannot find from resource: {:#?}", &from))
    }

    // Gets a list of parent -> child connections, always from parent to child
    pub fn get_parents(&self, from: &Resource) -> Result<Vec<(Resource, Resource)>> {
        // TODO: This will only match one possible traversal path
        let mut return_vec = Vec::new();
        match &from {
            Resource::Project(_) => return Err(anyhow!("Project does not have a parent")),
            Resource::Collection(_) => {
                for ref_val in self.relations_cache.iter() {
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
                for ref1_val in self.relations_cache.iter() {
                    if ref1_val.value().contains(from) {
                        for ref2_val in self.relations_cache.iter() {
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
                for ref1_val in self.relations_cache.iter() {
                    if ref1_val.value().contains(from) {
                        for ref2_val in self.relations_cache.iter() {
                            if ref2_val.value().contains(ref1_val.key()) {
                                for ref3_val in self.relations_cache.iter() {
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

    pub fn remove_all_res(&self, res: Resource) {
        self.relations_cache.remove(&res);
        for x in self.relations_cache.iter_mut() {
            x.value().remove(&res);
        }
    }

    fn add_name(&self, res: Resource, name: String) {
        self.name_cache.entry(name).or_default().insert(res);
    }

    fn remove_name(&self, res: Resource, name: Option<String>) {
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

    fn add_link(&self, from: Resource, to: Resource) -> Result<()> {
        match (&from, &to) {
            (&Resource::Project(_), &Resource::Collection(_)) => (),
            (&Resource::Project(_), &Resource::Dataset(_)) => (),
            (&Resource::Project(_), &Resource::Object(_)) => (),
            (&Resource::Collection(_), &Resource::Dataset(_)) => (),
            (&Resource::Collection(_), &Resource::Object(_)) => (),
            (&Resource::Dataset(_), &Resource::Object(_)) => (),
            (_, _) => return Err(anyhow!("Invalid pair from: {:#?}, to: {:#?}", from, to)),
        }
        let entry = self.relations_cache.entry(from).or_default();
        entry.insert(to);
        Ok(())
    }

    fn remove_link(&self, from: Resource, to: Resource) {
        let entry = self.relations_cache.entry(from).or_default();
        entry.remove(&to);
    }

    // Shared id cache functions !

    // Exchanges Shared -> Persistent or vice-versa
    pub fn add_or_update_shared(
        &self,
        shared: DieselUlid,
        persistent: DieselUlid,
        variant: ResourceVariant,
    ) -> Result<()> {
        match variant {
            ResourceVariant::Project => {
                if let Some(exists) = self
                    .shared_to_pid
                    .insert(shared, Resource::Project(persistent))
                {
                    self.pid_to_shared.remove(&exists.get_id());
                }
                self.pid_to_shared
                    .insert(persistent, Resource::Project(shared));
            }
            ResourceVariant::Collection => {
                if let Some(exists) = self
                    .shared_to_pid
                    .insert(shared, Resource::Collection(persistent))
                {
                    self.pid_to_shared.remove(&exists.get_id());
                }
                self.pid_to_shared
                    .insert(persistent, Resource::Collection(shared));
            }
            ResourceVariant::Dataset => {
                if let Some(exists) = self
                    .shared_to_pid
                    .insert(shared, Resource::Dataset(persistent))
                {
                    self.pid_to_shared.remove(&exists.get_id());
                }
                self.pid_to_shared
                    .insert(persistent, Resource::Dataset(shared));
            }
            ResourceVariant::Object => {
                if let Some(exists) = self
                    .shared_to_pid
                    .insert(shared, Resource::Object(persistent))
                {
                    self.pid_to_shared.remove(&exists.get_id());
                }
                self.pid_to_shared
                    .insert(persistent, Resource::Object(shared));
            }
            _ => return Err(anyhow!("Invalid variant")),
        }
        Ok(())
    }

    pub fn get_persistent(&self, shared: &DieselUlid) -> Option<Resource> {
        self.shared_to_pid.get(shared).map(|e| e.value().clone())
    }

    pub fn get_shared(&self, persistent: &DieselUlid) -> Option<Resource> {
        self.pid_to_shared
            .get(persistent)
            .map(|e| e.value().clone())
    }

    pub fn add_pubkey(&self, id: i32, pk: PubKey) {
        self.pubkeys.insert(id, pk);
    }

    pub fn remove_pubkey(&self, id: i32) {
        self.pubkeys.remove(&id);
    }

    pub fn set_pubkeys(&self, pks: Vec<Pubkey>) {
        self.pubkeys.clear();
        for pk in pks {
            let split = pk.location.split('_').collect::<Vec<_>>();
            if split.contains(&"proxy") {
                self.pubkeys.insert(
                    pk.id,
                    PubKey::DataProxy(split.last().unwrap_or(&"proxy").to_string()),
                );
            } else {
                self.pubkeys.insert(
                    pk.id,
                    PubKey::Server(split.last().unwrap_or(&"proxy").to_string()),
                );
            }
        }
    }

    pub fn get_pubkeys(&self) -> Vec<PubKey> {
        self.pubkeys
            .to_owned()
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    fn add_or_update_oidc(&self, oidc_id: String, user_id: DieselUlid) {
        self.oidc_ids.insert(oidc_id, user_id);
    }

    pub fn add_or_update_user(&self, user: User) -> Result<()> {
        let uid = DieselUlid::from_str(&user.id)?;
        let ext_id = user.external_ids.first().unwrap().external_id.clone();
        self.user_cache.insert(uid, user);
        self.add_or_update_oidc(ext_id, uid);
        Ok(())
    }

    pub fn get_user_by_oidc(&self, oidc_id: &str) -> Option<User> {
        self.oidc_ids
            .get(oidc_id)
            .and_then(|e| self.user_cache.get(&e.value()))
            .map(|e| e.value().clone())
    }

    pub fn get_user(&self, user_id: DieselUlid) -> Option<User> {
        self.user_cache.get(&user_id).map(|e| e.value().clone())
    }

    pub fn remove_user(&self, user_id: &DieselUlid) {
        self.user_cache.remove(user_id);
    }

    pub fn remove_resource(&self, persistent_resource: Resource, shared_id: DieselUlid) {
        self.object_cache.remove(&persistent_resource);
        self.remove_name(persistent_resource, None);
        let pid = self.shared_to_pid.get(&shared_id);
        if let Some(pi) = pid {
            self.pid_to_shared.remove(&pi.value().get_id());
        }
    }

    pub fn process_api_resource_update(
        &self,
        res: ApiResource,
        shared_id: DieselUlid,
        persistent_resource: Resource,
    ) -> Result<()> {
        if let Some(old) = self
            .object_cache
            .insert(persistent_resource.clone(), res.clone())
        {
            let (add_rel, remove_rel, name_update) =
                self.check_updates(&old, &res, persistent_resource.clone());

            if let Some((old_name, new_name)) = name_update {
                self.remove_name(persistent_resource.clone(), Some(old_name));
                self.add_name(persistent_resource.clone(), new_name);
            }

            for (from, to) in remove_rel {
                self.remove_link(from, to);
            }

            for (from, to) in add_rel {
                self.add_link(from, to)?;
            }
        }
        self.shared_to_pid
            .insert(shared_id, persistent_resource.clone());
        self.pid_to_shared.insert(
            persistent_resource.get_id(),
            persistent_resource.update_id(shared_id),
        );
        self.object_cache.insert(persistent_resource, res);
        Ok(())
    }

    fn check_updates(
        &self,
        old: &ApiResource,
        new: &ApiResource,
        res: Resource,
    ) -> (
        Vec<(Resource, Resource)>,
        Vec<(Resource, Resource)>,
        Option<(String, String)>,
    ) {
        let mut names = None;
        let mut add_relation = Vec::new();
        let mut remove_relation = Vec::new();

        match (old, new) {
            (ApiResource::Project(old_proj), ApiResource::Project(new_proj)) => {
                if old_proj.name != new_proj.name {
                    names = Some((old_proj.name.to_string(), new_proj.name.to_string()));
                }
                (add_relation, remove_relation) = self
                    .process_relations(&old_proj.relations, &new_proj.relations, res)
                    .unwrap_or_default();
            }
            (ApiResource::Collection(old_col), ApiResource::Collection(new_col)) => {
                if old_col.name != new_col.name {
                    names = Some((old_col.name.to_string(), new_col.name.to_string()));
                }
                (add_relation, remove_relation) = self
                    .process_relations(&old_col.relations, &new_col.relations, res)
                    .unwrap_or_default();
            }
            (ApiResource::Dataset(old_ds), ApiResource::Dataset(new_ds)) => {
                if old_ds.name != new_ds.name {
                    names = Some((old_ds.name.to_string(), new_ds.name.to_string()));
                }
                (add_relation, remove_relation) = self
                    .process_relations(&old_ds.relations, &new_ds.relations, res)
                    .unwrap_or_default();
            }
            (ApiResource::Object(old_obj), ApiResource::Object(new_obj)) => {
                if old_obj.name != new_obj.name {
                    names = Some((old_obj.name.to_string(), new_obj.name.to_string()));
                }
                (add_relation, remove_relation) = self
                    .process_relations(&old_obj.relations, &new_obj.relations, res)
                    .unwrap_or_default();
            }
            _ => (),
        }
        (add_relation, remove_relation, names)
    }

    fn process_relations(
        &self,
        old: &Vec<Relation>,
        new: &Vec<Relation>,
        origin: Resource,
    ) -> Result<(Vec<(Resource, Resource)>, Vec<(Resource, Resource)>)> {
        let mut new_rel = HashMap::default();
        let mut remove = Vec::new();

        for nrel in new {
            if let Some(relation::Relation::Internal(int)) = &nrel.relation {
                if int.defined_variant() == InternalRelationVariant::BelongsTo {
                    let (from, to) = internal_relation_to_rel(origin.clone(), int.clone())?;
                    new_rel.insert(from, to);
                }
            }
        }

        for orel in old {
            if let Some(relation::Relation::Internal(int)) = &orel.relation {
                if int.defined_variant() == InternalRelationVariant::BelongsTo {
                    let (from, to) = internal_relation_to_rel(origin.clone(), int.clone())?;
                    let new_rel_hit = new_rel.get(&from);
                    match new_rel_hit {
                        // If a new relation matches up an old one
                        Some(new_val) => {
                            // If both relations dont match up -> Relation change
                            if new_val != &to {
                                remove.push((from, to))
                            // Relations match up nothing must be done
                            } else {
                                new_rel.remove(&from);
                            }
                        }
                        // If no relation match up remove old one
                        None => remove.push((from, to)),
                    }
                }
            }
        }
        Ok((Vec::from_iter(new_rel), remove))
    }
}

#[cfg(test)]
mod tests {
    use aruna_rust_api::api::storage::models::v2::{
        permission::ResourceId, ExternalId, Permission, Token, UserAttributes,
    };

    use super::*;
    use crate::structs::Resource::*;

    #[test]
    fn test_new() {
        let cache = Cache::new();

        let cache2 = Cache {
            relations_cache: DashMap::with_hasher(RandomState::new()),
            name_cache: DashMap::with_hasher(RandomState::new()),
            shared_to_pid: DashMap::with_hasher(RandomState::new()),
            pid_to_shared: DashMap::with_hasher(RandomState::new()),
            object_cache: DashMap::with_hasher(RandomState::new()),
            user_cache: DashMap::with_hasher(RandomState::new()),
            pubkeys: DashMap::with_hasher(RandomState::new()),
            ..Default::default()
        };

        assert_eq!(format!("{:#?}", cache), format!("{:#?}", cache2))
    }

    #[test]
    fn test_traverse_fail() {
        let cache = Cache::new();

        assert!(cache
            .traverse_graph(&Resource::Collection(DieselUlid::generate()))
            .is_err());
    }

    fn generate_user(
        token_perm: Option<Permission>,
        personal_perm: Vec<Permission>,
    ) -> (DieselUlid, User) {
        let user_id = DieselUlid::generate();

        (
            user_id,
            User {
                id: user_id.to_string(),
                external_ids: vec![ExternalId {
                    external_id: "1".to_string(),
                    idp: "1".to_string(),
                }],
                display_name: "Name".to_string(),
                active: true,
                email: "t@t.t".to_string(),
                attributes: Some(UserAttributes {
                    global_admin: false,
                    service_account: false,
                    tokens: vec![Token {
                        id: DieselUlid::generate().to_string(),
                        name: "a_token".to_string(),
                        user_id: user_id.to_string(),
                        created_at: None,
                        expires_at: None,
                        permission: token_perm,
                        used_at: None,
                    }],
                    custom_attributes: vec![],
                    personal_permissions: personal_perm,
                }),
            },
        )
    }

    #[test]
    fn test_user() {
        let cache = Cache::new();

        let project_id = DieselUlid::generate();

        let (_id, user) = generate_user(
            None,
            vec![Permission {
                permission_level: 1,
                resource_id: Some(ResourceId::ProjectId(project_id.to_string())),
            }],
        );

        cache.add_or_update_user(user).unwrap();
    }

    #[test]
    fn test_shared() {
        let cache = Cache::new();

        let shared_1 = DieselUlid::generate();
        let persistent_1 = DieselUlid::generate();
        let persistent_2 = DieselUlid::generate();

        cache
            .add_or_update_shared(shared_1, persistent_1, ResourceVariant::Project)
            .unwrap();

        assert_eq!(
            cache.get_shared(&persistent_1).unwrap(),
            Resource::Project(shared_1)
        );

        assert_eq!(
            cache.get_persistent(&shared_1).unwrap(),
            Resource::Project(persistent_1)
        );

        cache
            .add_or_update_shared(shared_1, persistent_2, ResourceVariant::Project)
            .unwrap();

        assert_eq!(
            cache.get_shared(&persistent_2).unwrap(),
            Resource::Project(shared_1)
        );

        assert_eq!(
            cache.get_persistent(&shared_1).unwrap(),
            Resource::Project(persistent_2)
        );

        assert!(cache.shared_to_pid.len() == 1);
        assert!(cache.pid_to_shared.len() == 1);
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

        // Test the get_parents function
        let result = cache.get_parents(&resource_c).unwrap();
        let expected = vec![
            (resource_a.clone(), resource_b.clone()),
            (resource_b.clone(), resource_c.clone()),
        ];
        assert_eq!(result, expected);

        // Test the get_parents function
        let result = cache.get_parents(&resource_b).unwrap();
        let expected = vec![(resource_a.clone(), resource_b.clone())];
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
        cache.remove_name(resource_b.clone(), None);

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
        assert!(cache.relations_cache.get(&resource_a).unwrap().is_empty());
    }

    #[test]
    fn test_add_pubkey() {
        let cache = Cache::new();
        let pubkey = PubKey::DataProxy("pubkey".to_owned());

        // Test adding a pubkey
        cache.add_pubkey(1, pubkey.clone());

        // Check if the pubkey is present in the cache
        assert!(cache.pubkeys.contains_key(&1));
    }

    #[test]
    fn test_remove_pubkey() {
        let cache = Cache::new();
        let pubkey_a = PubKey::DataProxy("pubkey_a".to_owned());
        let pubkey_b = PubKey::DataProxy("pubkey_b".to_owned());

        // Add pubkeys to the cache
        cache.add_pubkey(1, pubkey_a.clone());
        cache.add_pubkey(2, pubkey_b.clone());

        // Test removing a pubkey
        cache.remove_pubkey(1);

        // Check if the pubkey is removed from the cache
        assert!(!cache.pubkeys.contains_key(&1));
    }

    #[test]
    fn test_get_pubkeys() {
        let cache = Cache::new();
        let pubkey = PubKey::DataProxy("pubkey".to_owned());

        // Test adding a pubkey
        cache.add_pubkey(1, pubkey.clone());

        // Check if the pubkey is present in the cache
        assert!(cache.pubkeys.contains_key(&1));

        assert_eq!(cache.get_pubkeys().len(), 1);
    }

    #[test]
    fn test_get_parents_with_target() {
        let cache = Cache::new();

        let project_a = Resource::Project(DieselUlid::generate());
        let project_b = Resource::Project(DieselUlid::generate());

        let collection_a = Resource::Collection(DieselUlid::generate());
        let collection_b = Resource::Collection(DieselUlid::generate());
        let collection_c = Resource::Collection(DieselUlid::generate());
        let collection_d = Resource::Collection(DieselUlid::generate());

        let dataset_a = Resource::Dataset(DieselUlid::generate());
        let object_a = Resource::Object(DieselUlid::generate());

        // Add entries to the graph cache
        cache
            .add_link(project_a.clone(), collection_a.clone())
            .unwrap();
        cache
            .add_link(project_a.clone(), collection_b.clone())
            .unwrap();
        cache
            .add_link(project_a.clone(), collection_c.clone())
            .unwrap();
        cache
            .add_link(project_a.clone(), collection_d.clone())
            .unwrap();
        cache
            .add_link(collection_a.clone(), dataset_a.clone())
            .unwrap();
        cache
            .add_link(collection_b.clone(), dataset_a.clone())
            .unwrap();
        cache
            .add_link(collection_c.clone(), dataset_a.clone())
            .unwrap();
        cache
            .add_link(collection_d.clone(), dataset_a.clone())
            .unwrap();
        cache.add_link(dataset_a.clone(), object_a.clone()).unwrap();

        assert!(cache
            .get_parents_with_targets(&object_a, vec![project_a.clone()])
            .is_ok());
        assert!(cache
            .get_parents_with_targets(&object_a, vec![collection_d.clone()])
            .is_ok());
        assert!(cache
            .get_parents_with_targets(&object_a, vec![dataset_a.clone()])
            .is_ok());
        assert!(cache
            .get_parents_with_targets(&object_a, vec![project_b.clone()])
            .is_err());
    }
}
