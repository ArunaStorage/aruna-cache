use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::event_notification_service_client::{
    self, EventNotificationServiceClient,
};
use aruna_rust_api::api::notification::services::v2::resource_event_context::Event;
use aruna_rust_api::api::notification::services::v2::AcknowledgeMessageBatchRequest;
use aruna_rust_api::api::notification::services::v2::EventMessage;
use aruna_rust_api::api::notification::services::v2::GetEventMessageBatchStreamRequest;
use aruna_rust_api::api::notification::services::v2::Reply;
use aruna_rust_api::api::notification::services::v2::ResourceEvent;
use aruna_rust_api::api::notification::services::v2::ResourceEventType;
use aruna_rust_api::api::storage::models::v2::relation::Relation;
use aruna_rust_api::api::storage::models::v2::RelationDirection;
use aruna_rust_api::api::storage::models::v2::ResourceVariant;
use diesel_ulid::DieselUlid;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

use crate::cache::Cache;
use crate::structs::Resource;

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
        EventNotificationServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    cache: Cache,
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
                channel,
                interceptor,
            );

        Ok(NotificationCache {
            notification_service,
            cache: Cache::new(),
        })
    }

    pub async fn create_channel(&mut self, streamgroup: String) -> Result<()> {
        let stream = self
            .notification_service
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
                .acknowledge_message_batch(Request::new(AcknowledgeMessageBatchRequest {
                    replies: acks,
                }))
                .await?;
        }
        Err(anyhow!("Stream was closed by sender"))
    }

    pub async fn process_message(&self, message: EventMessage) -> Option<Reply> {
        match message.message_variant.unwrap() {
            MessageVariant::ResourceEvent(r_event) => self.process_resource_event(r_event).await,
            MessageVariant::UserEvent(u_event) => u_event.reply,
            MessageVariant::AnnouncementEvent(a_event) => a_event.reply,
        }
    }

    pub async fn process_resource_event(&self, event: ResourceEvent) -> Option<Reply> {
        match event.event_type() {
            ResourceEventType::Created => {
                if let Some(r) = event.resource {
                    let (associated_id, res) = match r.resource_variant() {
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Project => (
                            DieselUlid::from_str(&r.resource_id).ok()?,
                            Resource::Project(DieselUlid::from_str(&r.associated_id).ok()?),
                        ),
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Collection => (
                            DieselUlid::from_str(&r.resource_id).ok()?,
                            Resource::Collection(DieselUlid::from_str(&r.associated_id).ok()?),
                        ),
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Dataset => (
                            DieselUlid::from_str(&r.resource_id).ok()?,
                            Resource::Dataset(DieselUlid::from_str(&r.associated_id).ok()?),
                        ),
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Object => (
                            DieselUlid::from_str(&r.associated_id).ok()?,
                            Resource::Dataset(DieselUlid::from_str(&r.resource_id).ok()?),
                        ),
                        _ => return None,
                    };
                    if let Some(ctx) = event.context {
                        if let Some(Event::RelationUpdates(new_relations)) = ctx.event {
                            for rel in new_relations.add_relations {
                                if let Some(Relation::Internal(int)) = rel.relation {
                                    if int.direction() == RelationDirection::Inbound {
                                        match int.resource_variant() {
                                            ResourceVariant::Project => self
                                                .cache
                                                .add_link(
                                                    Resource::Project(
                                                        self.cache.get_associated_id(
                                                            DieselUlid::from_str(&int.resource_id)
                                                                .ok()?,
                                                        )?,
                                                    ),
                                                    res.clone(),
                                                )
                                                .ok()?,
                                            ResourceVariant::Collection => self
                                                .cache
                                                .add_link(
                                                    Resource::Collection(
                                                        self.cache.get_associated_id(
                                                            DieselUlid::from_str(&int.resource_id)
                                                                .ok()?,
                                                        )?,
                                                    ),
                                                    res.clone(),
                                                )
                                                .ok()?,
                                            ResourceVariant::Dataset => self
                                                .cache
                                                .add_link(
                                                    Resource::Dataset(
                                                        self.cache.get_associated_id(
                                                            DieselUlid::from_str(&int.resource_id)
                                                                .ok()?,
                                                        )?,
                                                    ),
                                                    res.clone(),
                                                )
                                                .ok()?,
                                            _ => (),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    self.cache.add_shared(associated_id, res.get_id());
                    self.cache.add_name(res, r.resource_name);
                }
            }
            ResourceEventType::Available => {}
            ResourceEventType::Updated => {}
            ResourceEventType::Deleted => {}
            _ => (),
        }

        event.reply
    }
}
