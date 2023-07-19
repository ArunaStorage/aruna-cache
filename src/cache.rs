use crate::structs::PubKey;
use crate::structs::{Resource, ResourcePermission};
use crate::utils::internal_relation_to_rel;
use ahash::{HashMap, RandomState};
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use aruna_rust_api::api::storage::models::v2::{
    generic_resource::Resource as ApiResource, PermissionLevel,
};
use aruna_rust_api::api::storage::models::v2::{
    relation, InternalRelationVariant, Relation, ResourceVariant, User,
};
use aruna_rust_api::api::storage::services::v2::Pubkey;
use dashmap::{DashMap, DashSet};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

#[derive(Debug)]
pub struct Cache {
    // Graph cache contains From -> [all]
    pub graph_cache: DashMap<Resource, DashSet<Resource, RandomState>, RandomState>,
    pub name_cache: DashMap<String, DashSet<Resource, RandomState>, RandomState>,
    pub shared_to_pid: DashMap<DieselUlid, Resource, RandomState>,
    pub pid_to_shared: DashMap<DieselUlid, Resource, RandomState>,
    pub object_cache: DashMap<Resource, ApiResource, RandomState>,
    pub permissions:
        DashMap<DieselUlid, DashMap<ResourcePermission, PermissionLevel, RandomState>, RandomState>,
    pub pubkeys: DashMap<i32, PubKey, RandomState>,
    pub oidc_ids: DashMap<String, DieselUlid, RandomState>,
    pub token_ids: DashMap<DieselUlid, DieselUlid, RandomState>,
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
            shared_to_pid: DashMap::with_hasher(RandomState::new()),
            pid_to_shared: DashMap::with_hasher(RandomState::new()),
            object_cache: DashMap::with_hasher(RandomState::new()),
            permissions: DashMap::with_hasher(RandomState::new()),
            pubkeys: DashMap::with_hasher(RandomState::new()),
            oidc_ids: DashMap::with_hasher(RandomState::new()),
            token_ids: DashMap::with_hasher(RandomState::new()),
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

    pub fn get_parents_with_targets(&self, from: &Resource, targets: Vec<Resource>) -> Result<()> {
        match &from {
            Resource::Project(_) => return Err(anyhow!("Project does not have a parent")),
            Resource::Collection(_) => {
                for ref_val in self.graph_cache.iter() {
                    if ref_val.value().contains(from) && targets.contains(ref_val.key()) {
                        return Ok(());
                    }
                }
            }
            Resource::Dataset(_) => {
                for ref1_val in self.graph_cache.iter() {
                    if ref1_val.value().contains(from) {
                        if targets.contains(ref1_val.key()) {
                            return Ok(());
                        }
                        for ref2_val in self.graph_cache.iter() {
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
                for ref1_val in self.graph_cache.iter() {
                    if ref1_val.value().contains(from) {
                        if targets.contains(ref1_val.key()) {
                            return Ok(());
                        }
                        for ref2_val in self.graph_cache.iter() {
                            if ref2_val.value().contains(ref1_val.key()) {
                                if targets.contains(ref2_val.key()) {
                                    return Ok(());
                                }
                                for ref3_val in self.graph_cache.iter() {
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

    pub fn remove_all_res(&self, res: Resource) {
        self.graph_cache.remove(&res);
        for x in self.graph_cache.iter_mut() {
            x.value().remove(&res);
        }
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

    pub fn add_oidc(&self, oidc_id: String, user_id: DieselUlid) {
        self.oidc_ids.insert(oidc_id, user_id);
    }
    pub fn remove_oidc(&self, oidc_id: &str) {
        self.oidc_ids.remove(oidc_id);
    }
    pub fn get_user_perm_by_oidc(
        &self,
        oidc_id: &str,
    ) -> Option<Vec<(ResourcePermission, PermissionLevel)>> {
        let ulid = self.oidc_ids.get(oidc_id)?;
        self.get_permissions(&ulid)
    }

    pub fn add_user_token(&self, token_id: DieselUlid, user_id: DieselUlid) {
        self.token_ids.insert(token_id, user_id);
    }

    pub fn remove_user_token(&self, token_id: DieselUlid) {
        self.token_ids.remove(&token_id);
    }

    pub fn get_user_by_token(&self, token_id: DieselUlid) -> Option<DieselUlid> {
        self.token_ids.get(&token_id).map(|e| *e.value())
    }

    pub fn remove_all_tokens_by_user(&self, user_id: DieselUlid) {
        for (t, id) in self.token_ids.clone() {
            // Remove all tokens and permissions
            if id == user_id {
                self.permissions.remove(&t);
                self.token_ids.remove(&t);
            }
        }
    }

    pub fn parse_and_update_user_info(&self, uinfo: User) -> Option<()> {
        let uid = DieselUlid::from_str(&uinfo.id).ok()?;
        let user_attributes = uinfo.attributes?;

        self.remove_all_tokens_by_user(uid);
        let user_perm: DashMap<ResourcePermission, PermissionLevel, RandomState> =
            DashMap::with_hasher(RandomState::new());

        if user_attributes.global_admin {
            user_perm.insert(ResourcePermission::GlobalAdmin, PermissionLevel::Admin);
        }

        if user_attributes.service_account {
            user_perm.insert(ResourcePermission::ServiceAccount, PermissionLevel::None);
        }

        for p in user_attributes.personal_permissions {
            let res_id = p.clone().resource_id?;
            match res_id {
                ResourceId::ProjectId(pid) => user_perm.insert(
                    ResourcePermission::Resource(Resource::Project(
                        DieselUlid::from_str(&pid).ok()?,
                    )),
                    p.permission_level(),
                ),
                ResourceId::CollectionId(cid) => user_perm.insert(
                    ResourcePermission::Resource(Resource::Collection(
                        DieselUlid::from_str(&cid).ok()?,
                    )),
                    p.permission_level(),
                ),
                ResourceId::DatasetId(did) => user_perm.insert(
                    ResourcePermission::Resource(Resource::Dataset(
                        DieselUlid::from_str(&did).ok()?,
                    )),
                    p.permission_level(),
                ),
                ResourceId::ObjectId(oid) => user_perm.insert(
                    ResourcePermission::Resource(Resource::Object(
                        DieselUlid::from_str(&oid).ok()?,
                    )),
                    p.permission_level(),
                ),
            };
        }

        for t in user_attributes.tokens {
            let token_id = DieselUlid::from_str(&t.id).ok()?;
            match t.permission {
                Some(perm) => {
                    let map: DashMap<ResourcePermission, PermissionLevel, RandomState> =
                        DashMap::with_hasher(RandomState::new());
                    let res_id = perm.clone().resource_id?;
                    match res_id {
                        ResourceId::ProjectId(pid) => map.insert(
                            ResourcePermission::Resource(Resource::Project(
                                DieselUlid::from_str(&pid).ok()?,
                            )),
                            perm.permission_level(),
                        ),
                        ResourceId::CollectionId(cid) => map.insert(
                            ResourcePermission::Resource(Resource::Collection(
                                DieselUlid::from_str(&cid).ok()?,
                            )),
                            perm.permission_level(),
                        ),
                        ResourceId::DatasetId(did) => map.insert(
                            ResourcePermission::Resource(Resource::Dataset(
                                DieselUlid::from_str(&did).ok()?,
                            )),
                            perm.permission_level(),
                        ),
                        ResourceId::ObjectId(oid) => map.insert(
                            ResourcePermission::Resource(Resource::Object(
                                DieselUlid::from_str(&oid).ok()?,
                            )),
                            perm.permission_level(),
                        ),
                    };
                    self.permissions.insert(token_id, map);
                }
                None => {
                    self.add_user_token(uid, token_id);
                    self.permissions.insert(token_id, user_perm.clone());
                }
            }
        }

        Some(())
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
    use super::*;
    use crate::structs::Resource::*;

    #[test]
    fn test_new() {
        let cache = Cache::new();

        let cache2 = Cache {
            graph_cache: DashMap::with_hasher(RandomState::new()),
            name_cache: DashMap::with_hasher(RandomState::new()),
            shared_to_pid: DashMap::with_hasher(RandomState::new()),
            pid_to_shared: DashMap::with_hasher(RandomState::new()),
            object_cache: DashMap::with_hasher(RandomState::new()),
            permissions: DashMap::with_hasher(RandomState::new()),
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
        assert!(cache.graph_cache.get(&resource_a).unwrap().is_empty());
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
        cache.add_or_update_permission(resource_id, permission.clone());

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
        cache.add_or_update_permission(resource_id, updated_permission.clone());

        // Check if the permission is updated
        let result = cache
            .permissions
            .get(&resource_id)
            .map(|entry| entry.clone().into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let expected = vec![permission, (updated_permission.0, updated_permission.1)];
        assert!(result.iter().all(|item| expected.contains(item)));
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
        cache.add_or_update_permission(resource_id, permission.clone());

        // Test removing a specific permission
        cache.remove_permission(resource_id, Some(permission.0), true);

        // Check if the permission is removed for the specific resource
        assert!(!cache.permissions.contains_key(&resource_id));

        let permission = (
            ResourcePermission::Resource(Resource::Project(DieselUlid::generate())),
            PermissionLevel::Read,
        );
        cache.add_or_update_permission(resource_id, permission.clone());
        cache.remove_permission(resource_id, Some(permission.0), false);
        assert!(cache.permissions.get(&resource_id).unwrap().is_empty());
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
        cache.add_or_update_permission(resource_id, permission_1.clone());
        cache.add_or_update_permission(resource_id, permission_2.clone());

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

    #[test]
    fn test_oidc() {
        let cache = Cache::new();

        let name = "a_name".to_string();
        let id = DieselUlid::generate();

        cache.add_oidc(name.clone(), id);

        assert!(cache.get_user_perm_by_oidc(&name).is_none());

        cache.add_or_update_permission(
            id,
            (ResourcePermission::GlobalAdmin, PermissionLevel::Admin),
        );

        assert!(
            cache.get_user_perm_by_oidc(&name).unwrap()
                == vec![(ResourcePermission::GlobalAdmin, PermissionLevel::Admin)]
        );

        cache.remove_oidc(&name);
        assert!(cache.get_user_perm_by_oidc(&name).is_none());
    }

    #[test]
    fn test_token_association() {
        let cache = Cache::new();

        let uid = DieselUlid::generate();
        let tid = DieselUlid::generate();

        cache.add_user_token(tid, uid);

        assert_eq!(cache.get_user_by_token(tid).unwrap(), uid);

        cache.remove_user_token(tid);

        assert!(cache.get_user_by_token(tid).is_none());
    }
}
