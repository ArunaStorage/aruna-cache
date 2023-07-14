use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::event_notification_service_client::{
    self, EventNotificationServiceClient,
};
use aruna_rust_api::api::notification::services::v2::AcknowledgeMessageBatchRequest;
use aruna_rust_api::api::notification::services::v2::EventMessage;
use aruna_rust_api::api::notification::services::v2::GetEventMessageBatchStreamRequest;
use aruna_rust_api::api::notification::services::v2::Reply;
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

        return Ok(mut_req);
    }
}

pub struct NotificationCache {
    notification_service:
        EventNotificationServiceClient<InterceptedService<Channel, ClientInterceptor>>,
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
            notification_service: notification_service,
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

        loop {
            if let Some(m) = inner_stream.message().await? {
                let mut acks = Vec::new();
                for message in m.messages {
                    acks.push(self.process_message(message).await);
                }
                self.notification_service
                    .acknowledge_message_batch(Request::new(AcknowledgeMessageBatchRequest {
                        replies: acks,
                    }))
                    .await?;
            } else {
                break;
            }
        }

        Err(anyhow!("Stream was closed by sender"))
    }

    pub async fn process_message(&self, message: EventMessage) -> Reply {
        match message.message_variant.unwrap() {
            MessageVariant::ResourceEvent(r_event) => r_event.reply.unwrap(),
            MessageVariant::UserEvent(u_event) => u_event.reply.unwrap(),
            MessageVariant::AnnouncementEvent(a_event) => a_event.reply.unwrap(),
        }
    }
}
