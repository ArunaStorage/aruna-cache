use crate::cache::Cache;
use crate::query::QueryHandler;
use crate::utils::GetRef;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::anouncement_event;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::event_notification_service_client::{
    self, EventNotificationServiceClient,
};
use aruna_rust_api::api::notification::services::v2::AcknowledgeMessageBatchRequest;
use aruna_rust_api::api::notification::services::v2::AnouncementEvent;
use aruna_rust_api::api::notification::services::v2::EventMessage;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::notification::services::v2::GetEventMessageBatchStreamRequest;
use aruna_rust_api::api::notification::services::v2::Reply;
use aruna_rust_api::api::notification::services::v2::ResourceEvent;
use aruna_rust_api::api::notification::services::v2::UserEvent;
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
    query: Box<dyn QueryHandler + Send + Sync>,
    pub cache: Cache,
}

impl NotificationCache {
    pub async fn new(
        token: impl Into<String>,
        server: impl Into<String>,
        qhandler: Box<dyn QueryHandler + Send + Sync>,
    ) -> Result<Self> {
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

        Ok(NotificationCache {
            notification_service: Some(notification_service),
            query: qhandler,
            cache: Cache::new(),
        })
    }

    pub async fn create_notifications_channel(&mut self, stream_consumer: String) -> Result<()> {
        let stream = self
            .notification_service
            .as_mut()
            .ok_or_else(|| anyhow!("Missing notification client"))?
            .get_event_message_batch_stream(Request::new(GetEventMessageBatchStreamRequest {
                stream_consumer,
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
            anouncement_event::EventVariant::NewPubkey(_)
            | anouncement_event::EventVariant::RemovePubkey(_)
            | anouncement_event::EventVariant::NewDataProxyId(_)
            | anouncement_event::EventVariant::RemoveDataProxyId(_)
            | anouncement_event::EventVariant::UpdateDataProxyId(_) => {
                self.cache.set_pubkeys(self.query.get_pubkeys().await.ok()?);
            }
            anouncement_event::EventVariant::Downtime(_) => (),
            anouncement_event::EventVariant::Version(_) => (),
        }
        message.reply
    }

    async fn process_user_event(&self, message: UserEvent) -> Option<Reply> {
        match message.event_variant() {
            EventVariant::Created | EventVariant::Available | EventVariant::Updated => {
                let uid = DieselUlid::from_str(&message.user_id).ok()?;
                let user_info = self.query.get_user(uid, message.checksum).await.ok()?;
                self.cache.add_or_update_user(user_info).ok()?;
            }
            EventVariant::Deleted => {
                let uid = DieselUlid::from_str(&message.user_id).ok()?;
                self.cache.remove_user(&uid);
            }
            _ => (),
        }
        message.reply
    }

    async fn process_resource_event(&self, event: ResourceEvent) -> Option<Reply> {
        match event.event_variant() {
            EventVariant::Created | EventVariant::Updated => {
                if let Some(r) = event.resource {
                    let (shared_id, persistent_res) = r.get_ref()?;
                    let info = self
                        .query
                        .get_resource(&persistent_res, r.checksum)
                        .await
                        .ok()?;
                    self.cache
                        .process_api_resource_update(info, shared_id, persistent_res)
                        .ok()?
                }
            }
            EventVariant::Deleted => {
                if let Some(r) = event.resource {
                    let (associated_id, res) = r.get_ref()?;
                    self.cache.remove_resource(res, associated_id);
                }
            }
            _ => (),
        }
        event.reply
    }
}
