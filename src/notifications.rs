use crate::cache::Cache;
use crate::structs::Resource;
use crate::structs::ResourcePermission;
use crate::utils::GetRef;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::anouncement_event::EventVariant;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::event_notification_service_client::{
    self, EventNotificationServiceClient,
};
use aruna_rust_api::api::notification::services::v2::AcknowledgeMessageBatchRequest;
use aruna_rust_api::api::notification::services::v2::AnouncementEvent;
use aruna_rust_api::api::notification::services::v2::EventMessage;
use aruna_rust_api::api::notification::services::v2::GetEventMessageBatchStreamRequest;
use aruna_rust_api::api::notification::services::v2::Reply;
use aruna_rust_api::api::notification::services::v2::ResourceEvent;
use aruna_rust_api::api::notification::services::v2::UserEvent;
use aruna_rust_api::api::storage::models::v2::internal_relation::Variant;
use aruna_rust_api::api::storage::models::v2::relation::Relation;
use aruna_rust_api::api::storage::models::v2::InternalRelation;
use aruna_rust_api::api::storage::models::v2::PermissionLevel;
use aruna_rust_api::api::storage::models::v2::RelationDirection;
use aruna_rust_api::api::storage::models::v2::ResourceVariant;
use aruna_rust_api::api::storage::services::v2::collection_service_client;
use aruna_rust_api::api::storage::services::v2::collection_service_client::CollectionServiceClient;
use aruna_rust_api::api::storage::services::v2::dataset_service_client;
use aruna_rust_api::api::storage::services::v2::dataset_service_client::DatasetServiceClient;
use aruna_rust_api::api::storage::services::v2::endpoint_service_client;
use aruna_rust_api::api::storage::services::v2::endpoint_service_client::EndpointServiceClient;
use aruna_rust_api::api::storage::services::v2::object_service_client;
use aruna_rust_api::api::storage::services::v2::object_service_client::ObjectServiceClient;
use aruna_rust_api::api::storage::services::v2::project_service_client;
use aruna_rust_api::api::storage::services::v2::project_service_client::ProjectServiceClient;
use aruna_rust_api::api::storage::services::v2::user_service_client;
use aruna_rust_api::api::storage::services::v2::user_service_client::UserServiceClient;
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

// Create a client interceptor which always adds the specified api token to the request header
#[derive(Clone)]
pub struct ClientInterceptor {
    api_token: String,
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

pub struct NotificationCache {
    notification_service:
        Option<EventNotificationServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    project_service: Option<ProjectServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    collection_service:
        Option<CollectionServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    dataset_service: Option<DatasetServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    object_service: Option<ObjectServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    user_service: Option<UserServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    endpoint_service: Option<EndpointServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    pub cache: Cache,
}

impl NotificationCache {
    pub async fn new(token: impl Into<String>, server: impl Into<String>) -> Result<Self> {
        let tls_config = ClientTlsConfig::new();
        let endpoint = Channel::from_shared(server.into())?.tls_config(tls_config)?;
        let channel = endpoint.connect().await?;
        let interceptor = ClientInterceptor {
            api_token: token.into(),
        };

        let notification_service =
            event_notification_service_client::EventNotificationServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            );

        let project_service = project_service_client::ProjectServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let collection_service =
            collection_service_client::CollectionServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            );

        let dataset_service = dataset_service_client::DatasetServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let object_service = object_service_client::ObjectServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let user_service = user_service_client::UserServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let endpoint_service =
            endpoint_service_client::EndpointServiceClient::with_interceptor(channel, interceptor);

        Ok(NotificationCache {
            notification_service: Some(notification_service),
            project_service: Some(project_service),
            collection_service: Some(collection_service),
            dataset_service: Some(dataset_service),
            object_service: Some(object_service),
            user_service: Some(user_service),
            endpoint_service: Some(endpoint_service),
            cache: Cache::new(),
        })
    }

    pub async fn create_notifications_channel(&mut self, streamgroup: String) -> Result<()> {
        let stream = self
            .notification_service
            .as_mut()
            .ok_or_else(|| anyhow!("Missing notification client"))?
            .get_event_message_batch_stream(Request::new(GetEventMessageBatchStreamRequest {
                stream_group_id: streamgroup,
                batch_size: 10,
            }))
            .await?;

        let mut inner_stream = stream.into_inner();

        while let Some(m) = inner_stream.message().await? {
            let mut acks = Vec::new();
            for message in m.messages {
                if let Some(r) = self.process_message(message).await {
                    acks.push(r)
                }
            }
            self.notification_service
                .as_mut()
                .ok_or_else(|| anyhow!("Missing notification client"))?
                .acknowledge_message_batch(Request::new(AcknowledgeMessageBatchRequest {
                    replies: acks,
                }))
                .await?;
        }
        Err(anyhow!("Stream was closed by sender"))
    }

    async fn process_message(&self, message: EventMessage) -> Option<Reply> {
        match message.message_variant.unwrap() {
            MessageVariant::ResourceEvent(r_event) => self.process_resource_event(r_event).await,
            MessageVariant::UserEvent(u_event) => self.process_user_event(u_event).await,
            MessageVariant::AnnouncementEvent(a_event) => {
                self.process_announcements_event(a_event).await
            }
        }
    }

    async fn process_announcements_event(&self, message: AnouncementEvent) -> Option<Reply> {
        match message.event_variant? {
            EventVariant::NewDataProxy(newdp_event) => {
                self.cache
                    .add_pubkey(crate::structs::PubKey::DataProxy(newdp_event.pubkey));
            }
            EventVariant::RemoveDataProxy(rem_dp_event) => self
                .cache
                .remove_pubkey(crate::structs::PubKey::DataProxy(rem_dp_event.pubkey)),
            EventVariant::Pubkey(pk_event) => self
                .cache
                .add_pubkey(crate::structs::PubKey::Server(pk_event.pubkey)),
            _ => (),
        }
        message.reply
    }

    async fn process_user_event(&self, message: UserEvent) -> Option<Reply> {
        match message.event_type() {
            UserEventType::Created | UserEventType::Updated => {
                if let Some(ctx) = message.context {
                    if let Some(e) = ctx.event {
                        match e {
                            user_event_context::Event::Admin(_) => {
                                self.cache.add_or_update_permission(
                                    DieselUlid::from_str(&message.user_id).ok()?,
                                    (ResourcePermission::GlobalAdmin, PermissionLevel::Admin),
                                )
                            }
                            user_event_context::Event::Token(t) => {
                                if let Some(p) = t.permission {
                                    if let Some(r) = p.resource_id.clone() {
                                        if p.permission_level() != PermissionLevel::Unspecified {
                                            self.cache.add_or_update_permission(
                                                DieselUlid::from_str(&t.id).ok()?,
                                                (
                                                    Resource::try_from(r).ok()?.into(),
                                                    p.permission_level(),
                                                ),
                                            );
                                            self.cache.add_user_token(
                                                DieselUlid::from_str(&t.id).ok()?,
                                                DieselUlid::from_str(&message.user_id).ok()?,
                                            );
                                        } else {
                                            let user_perm = self.cache.get_permissions(
                                                &DieselUlid::from_str(&message.user_id).ok()?,
                                            )?;

                                            for perm in user_perm {
                                                self.cache.add_or_update_permission(
                                                    DieselUlid::from_str(&t.id).ok()?,
                                                    perm,
                                                )
                                            }
                                            self.cache.add_user_token(
                                                DieselUlid::from_str(&t.id).ok()?,
                                                DieselUlid::from_str(&message.user_id).ok()?,
                                            );
                                        }
                                    }
                                }
                            }
                            user_event_context::Event::Permission(perm) => {
                                self.cache.add_or_update_permission(
                                    DieselUlid::from_str(&message.user_id).ok()?,
                                    (
                                        Resource::try_from(perm.resource_id.clone()?).ok()?.into(),
                                        perm.permission_level(),
                                    ),
                                );
                            }
                            _ => (),
                        }
                    }
                }
            }
            UserEventType::Deleted => {
                if let Some(ctx) = message.context {
                    if let Some(e) = ctx.event {
                        match e {
                            user_event_context::Event::Admin(_) => self.cache.remove_permission(
                                DieselUlid::from_str(&message.user_id).ok()?,
                                Some(ResourcePermission::GlobalAdmin),
                                false,
                            ),
                            user_event_context::Event::Token(t) => {
                                self.cache.remove_permission(
                                    DieselUlid::from_str(&t.id).ok()?,
                                    None,
                                    true,
                                );
                                self.cache
                                    .remove_user_token(DieselUlid::from_str(&t.id).ok()?);
                            }
                            user_event_context::Event::Permission(perm) => {
                                self.cache.remove_permission(
                                    DieselUlid::from_str(&message.user_id).ok()?,
                                    Some(
                                        Resource::try_from(perm.resource_id.clone()?).ok()?.into(),
                                    ),
                                    false,
                                );
                            }
                            _ => (),
                        }
                    }
                }
            }
            _ => (),
        }
        message.reply
    }

    async fn process_resource_event(&self, event: ResourceEvent) -> Option<Reply> {
        match event.event_type() {
            ResourceEventType::Created => {
                if let Some(r) = event.resource {
                    let (associated_id, res) = r.get_ref()?;
                    if let Some(ctx) = event.context {
                        if let Some(Event::RelationUpdates(new_relations)) = ctx.event {
                            self.process_relation_update(res.clone(), new_relations)?;
                        }
                    }
                    self.cache.add_shared(associated_id, res.get_id());
                    self.cache.add_name(res, r.resource_name);
                }
            }
            ResourceEventType::Updated => {
                if let Some(r) = event.resource {
                    let (_associated_id, res) = r.get_ref()?;
                    if let Some(ctx) = event.context {
                        match ctx.event? {
                            Event::RelationUpdates(new_relations) => {
                                self.process_relation_update(res, new_relations)?;
                            }
                            Event::UpdatedFields(fields) => {
                                if fields.updated_fields.contains(&String::from("name")) {
                                    self.cache.remove_name(res.clone(), None);
                                    self.cache.add_name(res, r.resource_name)
                                }
                            }
                            _ => (),
                        }
                    }
                }
            }
            ResourceEventType::Deleted => {
                if let Some(r) = event.resource {
                    let (_associated_id, res) = r.get_ref()?;
                    self.cache.remove_name(res.clone(), None);
                    self.cache.remove_all_res(res)
                }
            }
            _ => (),
        }

        event.reply
    }

    fn process_relation_update(&self, res: Resource, update: RelationUpdate) -> Option<()> {
        for rel in update.add_relations {
            if let Some(Relation::Internal(int)) = rel.relation {
                if let Some(Variant::DefinedVariant(1)) = int.variant {
                    match int.direction() {
                        RelationDirection::Inbound => self.add_relation(true, int, res.clone())?,
                        RelationDirection::Outbound => {
                            self.add_relation(false, int, res.clone())?
                        }
                        _ => return None,
                    }
                }
            }
        }
        for rel in update.remove_relations {
            if let Some(Relation::Internal(int)) = rel.relation {
                if let Some(Variant::DefinedVariant(1)) = int.variant {
                    match int.direction() {
                        RelationDirection::Inbound => {
                            self.remove_relation(true, int, res.clone())?
                        }
                        RelationDirection::Outbound => {
                            self.remove_relation(false, int, res.clone())?
                        }
                        _ => return None,
                    }
                }
            }
        }
        Some(())
    }

    fn remove_relation(&self, inbound: bool, int: InternalRelation, res: Resource) -> Option<()> {
        let res_id = DieselUlid::from_str(&int.resource_id).ok()?;
        let a_res_id = self
            .cache
            .get_associated_id(&DieselUlid::from_str(&int.resource_id).ok()?)?;
        if inbound {
            match int.resource_variant() {
                ResourceVariant::Project => self
                    .cache
                    .remove_link(Resource::Project(res_id), res.clone()),
                ResourceVariant::Collection => self
                    .cache
                    .remove_link(Resource::Collection(res_id), res.clone()),
                ResourceVariant::Dataset => self
                    .cache
                    .remove_link(Resource::Dataset(res_id), res.clone()),
                _ => (),
            }
        } else {
            match int.resource_variant() {
                ResourceVariant::Collection => self
                    .cache
                    .remove_link(res.clone(), Resource::Collection(res_id)),
                ResourceVariant::Dataset => self
                    .cache
                    .remove_link(res.clone(), Resource::Dataset(res_id)),
                ResourceVariant::Object => self
                    .cache
                    .remove_link(res.clone(), Resource::Object(a_res_id)),
                _ => (),
            }
        }
        Some(())
    }

    fn add_relation(&self, inbound: bool, int: InternalRelation, res: Resource) -> Option<()> {
        let res_id = DieselUlid::from_str(&int.resource_id).ok()?;
        let a_res_id = self
            .cache
            .get_associated_id(&DieselUlid::from_str(&int.resource_id).ok()?)?;
        if inbound {
            match int.resource_variant() {
                ResourceVariant::Project => self
                    .cache
                    .add_link(Resource::Project(res_id), res.clone())
                    .ok()?,
                ResourceVariant::Collection => self
                    .cache
                    .add_link(Resource::Collection(res_id), res.clone())
                    .ok()?,
                ResourceVariant::Dataset => self
                    .cache
                    .add_link(Resource::Dataset(res_id), res.clone())
                    .ok()?,
                _ => (),
            }
        } else {
            match int.resource_variant() {
                ResourceVariant::Collection => self
                    .cache
                    .add_link(res.clone(), Resource::Collection(res_id))
                    .ok()?,
                ResourceVariant::Dataset => self
                    .cache
                    .add_link(res.clone(), Resource::Dataset(res_id))
                    .ok()?,
                ResourceVariant::Object => self
                    .cache
                    .add_link(res.clone(), Resource::Object(a_res_id))
                    .ok()?,
                _ => (),
            }
        }
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::Cache;
    use super::NotificationCache;
    use super::*;
    use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
    use aruna_rust_api::api::notification::services::v2::resource_event_context::Event;
    use aruna_rust_api::api::notification::services::v2::DataproxyInfo;
    use aruna_rust_api::api::notification::services::v2::EventMessage;
    use aruna_rust_api::api::notification::services::v2::NewPubkey;
    use aruna_rust_api::api::notification::services::v2::RelationUpdate;
    use aruna_rust_api::api::notification::services::v2::Reply;
    use aruna_rust_api::api::notification::services::v2::Resource as APIResource;
    use aruna_rust_api::api::notification::services::v2::ResourceEvent;
    use aruna_rust_api::api::notification::services::v2::ResourceEventContext;
    use aruna_rust_api::api::notification::services::v2::ResourceEventType;
    use aruna_rust_api::api::notification::services::v2::Token;
    use aruna_rust_api::api::notification::services::v2::UserEventContext;
    use aruna_rust_api::api::storage::models::v2::internal_relation;
    use aruna_rust_api::api::storage::models::v2::internal_relation::Variant;
    use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
    use aruna_rust_api::api::storage::models::v2::relation;
    use aruna_rust_api::api::storage::models::v2::InternalRelation;
    use aruna_rust_api::api::storage::models::v2::InternalRelationVariant;
    use aruna_rust_api::api::storage::models::v2::Permission;
    use aruna_rust_api::api::storage::models::v2::Relation;
    use aruna_rust_api::api::storage::models::v2::RelationDirection;
    use aruna_rust_api::api::storage::models::v2::ResourceVariant;
    use diesel_ulid::DieselUlid;
    use std::str::FromStr;

    fn mtemplate(res: APIResource, irel: Vec<Relation>) -> EventMessage {
        EventMessage {
            message_variant: Some(MessageVariant::ResourceEvent(ResourceEvent {
                resource: Some(res),
                event_type: ResourceEventType::Created as i32,
                context: Some(ResourceEventContext {
                    event: Some(Event::RelationUpdates(RelationUpdate {
                        add_relations: irel,
                        remove_relations: vec![],
                    })),
                }),
                reply: Some(Reply {
                    reply: "a_reply".into(),
                    salt: "a_salt".into(),
                    hmac: "a_hmac".into(),
                }),
            })),
        }
    }

    #[tokio::test]
    async fn notification_processing_test() {
        let not_cache = NotificationCache {
            notification_service: None,
            cache: Cache::new(),
        };

        let id = DieselUlid::generate();
        let aid = DieselUlid::generate();

        let mut res = APIResource {
            resource_id: id.to_string(),
            resource_name: "a_resource".into(),
            associated_id: aid.to_string(),
            resource_variant: ResourceVariant::Project as i32,
        };

        let irel = vec![Relation {
            relation: Some(relation::Relation::Internal(InternalRelation {
                resource_id: aid.to_string(),
                resource_variant: ResourceVariant::Project.into(),
                direction: RelationDirection::Inbound.into(),
                variant: Some(Variant::DefinedVariant(
                    InternalRelationVariant::BelongsTo.into(),
                )),
            })),
        }];

        {
            let eirel = Vec::new();
            let reply = not_cache
                .process_message(mtemplate(res.clone(), eirel))
                .await
                .unwrap();
            assert_eq!(
                reply,
                Reply {
                    reply: "a_reply".into(),
                    salt: "a_salt".into(),
                    hmac: "a_hmac".into(),
                }
            );
        }
        assert_eq!(not_cache.cache.get_associated_id(&id).unwrap(), aid);
        assert_eq!(not_cache.cache.get_associated_id(&aid).unwrap(), id);

        let res_id = DieselUlid::generate();
        let as_id = DieselUlid::generate();
        res.resource_variant = ResourceVariant::Object.into();
        res.resource_id = res_id.to_string();
        res.associated_id = as_id.to_string();
        let reply = not_cache
            .process_message(mtemplate(res.clone(), irel))
            .await
            .unwrap();
        assert_eq!(
            reply,
            Reply {
                reply: "a_reply".into(),
                salt: "a_salt".into(),
                hmac: "a_hmac".into(),
            }
        );

        assert_eq!(not_cache.cache.get_associated_id(&res_id).unwrap(), as_id);
        assert_eq!(not_cache.cache.get_associated_id(&as_id).unwrap(), res_id);

        dbg!(&not_cache.cache.graph_cache);
        assert_eq!(
            not_cache
                .cache
                .get_parents(&crate::structs::Resource::Object(res_id.clone()))
                .unwrap(),
            not_cache
                .cache
                .traverse_graph(&crate::structs::Resource::Project(aid))
                .unwrap()
        );
    }

    fn create_relation_update(
        resource_id: &str,
        resource_variant: ResourceVariant,
    ) -> RelationUpdate {
        let internal_relation = InternalRelation {
            resource_id: resource_id.to_owned(),
            resource_variant: resource_variant as i32,
            direction: RelationDirection::Inbound as i32,
            variant: Some(internal_relation::Variant::DefinedVariant(
                InternalRelationVariant::BelongsTo as i32,
            )),
        };

        RelationUpdate {
            add_relations: vec![Relation {
                relation: Some(relation::Relation::Internal(internal_relation)),
            }],
            remove_relations: vec![],
        }
    }

    fn create_resource_event(
        event_type: ResourceEventType,
        resource: APIResource,
        context: Option<ResourceEventContext>,
        reply: Option<Reply>,
    ) -> EventMessage {
        EventMessage {
            message_variant: Some(MessageVariant::ResourceEvent(ResourceEvent {
                event_type: event_type as i32,
                resource: Some(resource),
                context,
                reply,
            })),
        }
    }

    #[tokio::test]
    async fn test_process_message_resource_event_created() {
        let cache = Cache::new();
        let notification_cache = NotificationCache {
            notification_service: None,
            cache,
        };

        let id = DieselUlid::generate();
        let associated_id = DieselUlid::generate();
        let resource = APIResource {
            resource_id: id.to_string(),
            resource_name: "aproj".to_string(),
            associated_id: DieselUlid::generate().to_string(),
            resource_variant: ResourceVariant::Project as i32,
        };
        let _relation_update =
            create_relation_update(&associated_id.to_string(), ResourceVariant::Project);
        let event_message =
            create_resource_event(ResourceEventType::Created, resource.clone(), None, None);

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(result, None);
        assert_eq!(notification_cache.cache.shared_id_cache.len(), 2);
        assert_eq!(notification_cache.cache.name_cache.len(), 1);
        assert_eq!(notification_cache.cache.graph_cache.len(), 0); // No relations added in this case
    }

    #[tokio::test]
    async fn test_process_message_resource_event_with_relation_updates() {
        let cache = Cache::new();
        let notification_cache = NotificationCache {
            notification_service: None,
            cache: cache,
        };

        let project_id = DieselUlid::generate();
        let collection_id = DieselUlid::generate();
        let resource = APIResource {
            resource_id: collection_id.to_string(),
            resource_name: "a_col".to_string(),
            associated_id: DieselUlid::generate().to_string(),
            resource_variant: ResourceVariant::Collection as i32,
        };
        let relation_update =
            create_relation_update(&project_id.to_string(), ResourceVariant::Project);
        let event_message = create_resource_event(
            ResourceEventType::Created,
            resource.clone(),
            Some(ResourceEventContext {
                event: Some(Event::RelationUpdates(relation_update)),
            }),
            None,
        );

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(result, None);
        assert_eq!(notification_cache.cache.shared_id_cache.len(), 0);
        assert_eq!(notification_cache.cache.name_cache.len(), 0);
        assert_eq!(notification_cache.cache.graph_cache.len(), 0);

        // let parents = notification_cache.cache
        //     .get_parents(&Resource::Collection(collection_id))
        //     .unwrap();
        // assert_eq!(
        //     parents,
        //     vec![(
        //         Resource::Project(project_id),
        //         Resource::Collection(collection_id)
        //     )]
        // );
    }

    #[tokio::test]
    async fn test_process_message_user_event_admin() {
        let cache = Cache::new();
        let notification_cache = NotificationCache {
            notification_service: None,
            cache: cache,
        };

        let user_id = DieselUlid::generate();
        let event_message = EventMessage {
            message_variant: Some(MessageVariant::UserEvent(UserEvent {
                user_id: user_id.to_string(),
                user_name: "a_name".to_string(),
                event_type: UserEventType::Created as i32,
                context: Some(UserEventContext {
                    event: Some(user_event_context::Event::Admin(true)),
                }),
                reply: Some(Reply {
                    reply: "a".to_string(),
                    salt: "b".to_string(),
                    hmac: "c".to_string(),
                }),
            })),
        };

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(
            result,
            Some(Reply {
                reply: "a".to_string(),
                salt: "b".to_string(),
                hmac: "c".to_string()
            })
        );
        assert_eq!(notification_cache.cache.permissions.len(), 1);

        let user_ulid = DieselUlid::from_str(&user_id.to_string()).unwrap();
        let permissions = notification_cache
            .cache
            .get_permissions(&user_ulid)
            .unwrap();
        assert_eq!(
            permissions,
            vec![(ResourcePermission::GlobalAdmin, PermissionLevel::Admin)]
        );
    }

    #[tokio::test]
    async fn test_process_message_user_event_token() {
        let notification_cache = NotificationCache {
            notification_service: None,
            cache: Cache::new(),
        };

        let token_id = DieselUlid::generate();
        let user_id = DieselUlid::generate();
        let event_message = EventMessage {
            message_variant: Some(MessageVariant::UserEvent(UserEvent {
                user_id: user_id.to_string(),
                user_name: "a_name".to_string(),
                event_type: UserEventType::Created as i32,
                context: Some(UserEventContext {
                    event: Some(user_event_context::Event::Token(Token {
                        id: token_id.to_string(),
                        permission: Some(Permission {
                            permission_level: PermissionLevel::Admin as i32,
                            resource_id: Some(ResourceId::CollectionId(
                                DieselUlid::generate().to_string(),
                            )),
                        }),
                    })),
                }),
                reply: Some(Reply {
                    reply: "a".to_string(),
                    salt: "b".to_string(),
                    hmac: "c".to_string(),
                }),
            })),
        };

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(
            result,
            Some(Reply {
                reply: "a".to_string(),
                salt: "b".to_string(),
                hmac: "c".to_string()
            })
        );

        assert!(notification_cache
            .cache
            .get_permissions(&token_id)
            .is_some());
    }

    #[tokio::test]
    async fn test_process_message_user_event_token_update() {
        let notification_cache = NotificationCache {
            notification_service: None,
            cache: Cache::new(),
        };

        let token_id = DieselUlid::generate();
        let user_id = DieselUlid::generate();
        let event_message = EventMessage {
            message_variant: Some(MessageVariant::UserEvent(UserEvent {
                user_id: user_id.to_string(),
                user_name: "a_name".to_string(),
                event_type: UserEventType::Updated as i32,
                context: Some(UserEventContext {
                    event: Some(user_event_context::Event::Token(Token {
                        id: token_id.to_string(),
                        permission: Some(Permission {
                            permission_level: PermissionLevel::Admin as i32,
                            resource_id: Some(ResourceId::CollectionId(
                                DieselUlid::generate().to_string(),
                            )),
                        }),
                    })),
                }),
                reply: Some(Reply {
                    reply: "a".to_string(),
                    salt: "b".to_string(),
                    hmac: "c".to_string(),
                }),
            })),
        };

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(
            result,
            Some(Reply {
                reply: "a".to_string(),
                salt: "b".to_string(),
                hmac: "c".to_string()
            })
        );

        assert!(notification_cache
            .cache
            .get_permissions(&token_id)
            .is_some());
    }

    #[tokio::test]
    async fn test_process_message_user_event_token_deleted() {
        let notification_cache = NotificationCache {
            notification_service: None,
            cache: Cache::new(),
        };

        let user_id = DieselUlid::generate();
        let event_message = EventMessage {
            message_variant: Some(MessageVariant::UserEvent(UserEvent {
                user_id: user_id.to_string(),
                user_name: "a_name".to_string(),
                event_type: UserEventType::Deleted as i32,
                context: None,
                reply: Some(Reply {
                    reply: "a".to_string(),
                    salt: "b".to_string(),
                    hmac: "c".to_string(),
                }),
            })),
        };

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(
            result,
            Some(Reply {
                reply: "a".to_string(),
                salt: "b".to_string(),
                hmac: "c".to_string()
            })
        );

        assert!(notification_cache.cache.get_permissions(&user_id).is_none());
    }

    #[tokio::test]
    async fn test_anouncement_events() {
        let notification_cache = NotificationCache {
            notification_service: None,
            cache: Cache::new(),
        };

        let _user_id = DieselUlid::generate();
        let event_message = EventMessage {
            message_variant: Some(MessageVariant::AnnouncementEvent(AnouncementEvent {
                event_variant: Some(EventVariant::NewDataProxy(DataproxyInfo {
                    endpoint_id: DieselUlid::generate().to_string(),
                    name: "a_name".to_string(),
                    pubkey: "A pubkey".to_string(),
                    ..Default::default()
                })),
                reply: Some(Reply {
                    reply: "a".to_string(),
                    salt: "b".to_string(),
                    hmac: "c".to_string(),
                }),
            })),
        };

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(
            result,
            Some(Reply {
                reply: "a".to_string(),
                salt: "b".to_string(),
                hmac: "c".to_string()
            })
        );

        assert_eq!(notification_cache.cache.get_pubkeys().len(), 1);

        let event_message = EventMessage {
            message_variant: Some(MessageVariant::AnnouncementEvent(AnouncementEvent {
                event_variant: Some(EventVariant::Pubkey(NewPubkey {
                    pubkey: "pubkey_2".to_string(),
                })),
                reply: Some(Reply {
                    reply: "a".to_string(),
                    salt: "b".to_string(),
                    hmac: "c".to_string(),
                }),
            })),
        };

        let result = notification_cache.process_message(event_message).await;

        assert_eq!(
            result,
            Some(Reply {
                reply: "a".to_string(),
                salt: "b".to_string(),
                hmac: "c".to_string()
            })
        );

        assert_eq!(notification_cache.cache.get_pubkeys().len(), 2);

        let event_message = EventMessage {
            message_variant: Some(MessageVariant::AnnouncementEvent(AnouncementEvent {
                event_variant: Some(EventVariant::RemoveDataProxy(DataproxyInfo {
                    endpoint_id: DieselUlid::generate().to_string(),
                    name: "a_name".to_string(),
                    pubkey: "A pubkey".to_string(),
                    ..Default::default()
                })),
                reply: Some(Reply {
                    reply: "a".to_string(),
                    salt: "b".to_string(),
                    hmac: "c".to_string(),
                }),
            })),
        };

        let _result = notification_cache.process_message(event_message).await;

        assert_eq!(notification_cache.cache.get_pubkeys().len(), 1);
    }
}
