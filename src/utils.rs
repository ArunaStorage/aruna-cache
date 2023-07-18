use crate::structs::Resource;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::{generic_resource, User};
use aruna_rust_api::api::{
    notification::services::v2::Resource as ApiResource,
    storage::models::v2::{InternalRelation, RelationDirection, ResourceVariant},
};
use base64::{engine::general_purpose, Engine};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use xxhash_rust::xxh3::xxh3_128;

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

pub fn internal_relation_to_rel(
    id: Resource,
    int_rel: InternalRelation,
) -> Result<(Resource, Resource)> {
    match int_rel.direction() {
        RelationDirection::Inbound => match int_rel.resource_variant() {
            ResourceVariant::Project => Ok((
                Resource::Project(DieselUlid::from_str(&int_rel.resource_id)?),
                id,
            )),
            ResourceVariant::Collection => Ok((
                Resource::Collection(DieselUlid::from_str(&int_rel.resource_id)?),
                id,
            )),
            ResourceVariant::Dataset => Ok((
                Resource::Dataset(DieselUlid::from_str(&int_rel.resource_id)?),
                id,
            )),
            ResourceVariant::Object => Ok((
                Resource::Object(DieselUlid::from_str(&int_rel.resource_id)?),
                id,
            )),
            _ => Err(anyhow!("Invalid resource variant")),
        },
        RelationDirection::Outbound => match int_rel.resource_variant() {
            ResourceVariant::Project => Ok((
                id,
                Resource::Project(DieselUlid::from_str(&int_rel.resource_id)?),
            )),
            ResourceVariant::Collection => Ok((
                id,
                Resource::Collection(DieselUlid::from_str(&int_rel.resource_id)?),
            )),
            ResourceVariant::Dataset => Ok((
                id,
                Resource::Dataset(DieselUlid::from_str(&int_rel.resource_id)?),
            )),
            ResourceVariant::Object => Ok((
                id,
                Resource::Object(DieselUlid::from_str(&int_rel.resource_id)?),
            )),
            _ => Err(anyhow!("Invalid resource variant")),
        },
        _ => Err(anyhow!("Invalid resource variant")),
    }
}

// Create a client interceptor which always adds the specified api token to the request header
#[derive(Clone)]
pub struct ClientInterceptor {
    pub api_token: String,
}
// Implement a request interceptor which always adds
//  the authorization header with a specific API token to all requests
impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut mut_req: tonic::Request<()> = request;
        let metadata = mut_req.metadata_mut();
        metadata.append(
            AsciiMetadataKey::from_bytes("authorization".as_bytes()).unwrap(),
            AsciiMetadataValue::try_from(format!("Bearer {}", self.api_token.as_str())).unwrap(),
        );

        Ok(mut_req)
    }
}

pub fn checksum_resource(gen_res: generic_resource::Resource) -> Result<String> {
    match gen_res {
        generic_resource::Resource::Project(mut proj) => {
            proj.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(xxh3_128(&bincode::serialize(&proj)?).to_be_bytes())
                .to_string())
        }
        generic_resource::Resource::Collection(mut col) => {
            col.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(xxh3_128(&bincode::serialize(&col)?).to_be_bytes())
                .to_string())
        }
        generic_resource::Resource::Dataset(mut ds) => {
            ds.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(xxh3_128(&bincode::serialize(&ds)?).to_be_bytes())
                .to_string())
        }
        generic_resource::Resource::Object(obj) => Ok(general_purpose::STANDARD_NO_PAD
            .encode(xxh3_128(&bincode::serialize(&obj)?).to_be_bytes())
            .to_string()),
    }
}

pub fn checksum_user(user: &User) -> Result<String> {
    Ok(general_purpose::STANDARD_NO_PAD
        .encode(xxh3_128(&bincode::serialize(user)?).to_be_bytes())
        .to_string())
}
