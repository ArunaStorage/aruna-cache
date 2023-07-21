use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::{generic_resource, User};
use base64::{engine::general_purpose, Engine};
use xxhash_rust::xxh3::xxh3_128;

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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use aruna_rust_api::api::storage::models::v2::{generic_resource, Project, User};
//     use diesel_ulid::DieselUlid;

//     #[test]
//     fn checksum_user_test() {
//         let user = User {
//             id: DieselUlid::generate().to_string(),
//             external_ids: todo!(),
//             display_name: todo!(),
//             active: todo!(),
//             email: todo!(),
//             attributes: todo!(),
//         };

//         let checksum = checksum_user(&user).unwrap();
//     }
// }
