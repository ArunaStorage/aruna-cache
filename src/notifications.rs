use crate::cache::Cache;
use crate::persistence::Persistence;
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
use aruna_rust_api::api::storage::services::v2::storage_status_service_client;
use aruna_rust_api::api::storage::services::v2::storage_status_service_client::StorageStatusServiceClient;
use aruna_rust_api::api::storage::services::v2::user_service_client;
use aruna_rust_api::api::storage::services::v2::user_service_client::UserServiceClient;
use aruna_rust_api::api::storage::services::v2::GetCollectionRequest;
use aruna_rust_api::api::storage::services::v2::GetDatasetRequest;
use aruna_rust_api::api::storage::services::v2::GetObjectRequest;
use aruna_rust_api::api::storage::services::v2::GetProjectRequest;
use aruna_rust_api::api::storage::services::v2::GetPubkeysRequest;
use aruna_rust_api::api::storage::services::v2::GetUserRedactedRequest;
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
    storage_status_service:
        Option<StorageStatusServiceClient<InterceptedService<Channel, ClientInterceptor>>>,
    persistence: Option<Persistence>,
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

        let endpoint_service = endpoint_service_client::EndpointServiceClient::with_interceptor(
            channel.clone(),
            interceptor.clone(),
        );

        let storage_status_service =
            storage_status_service_client::StorageStatusServiceClient::with_interceptor(
                channel,
                interceptor,
            );

        Ok(NotificationCache {
            notification_service: Some(notification_service),
            project_service: Some(project_service),
            collection_service: Some(collection_service),
            dataset_service: Some(dataset_service),
            object_service: Some(object_service),
            user_service: Some(user_service),
            endpoint_service: Some(endpoint_service),
            storage_status_service: Some(storage_status_service),
            persistence: None,
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
            anouncement_event::EventVariant::NewPubkey(_)
            | anouncement_event::EventVariant::RemovePubkey(_)
            | anouncement_event::EventVariant::NewDataProxyId(_)
            | anouncement_event::EventVariant::RemoveDataProxyId(_)
            | anouncement_event::EventVariant::UpdateDataProxyId(_) => {
                self.cache.set_pubkeys(
                    self.storage_status_service
                        .clone()
                        .as_mut()?
                        .get_pubkeys(Request::new(GetPubkeysRequest {}))
                        .await
                        .ok()?
                        .into_inner(),
                );
            }
            anouncement_event::EventVariant::Downtime(_) => (),
            anouncement_event::EventVariant::Version(_) => (),
        }
        message.reply
    }

    async fn process_user_event(&self, message: UserEvent) -> Option<Reply> {
        match message.event_variant() {
            EventVariant::Created | EventVariant::Available | EventVariant::Updated => {
                let user_info = self
                    .user_service
                    .clone()
                    .as_mut()?
                    .get_user_redacted(Request::new(GetUserRedactedRequest {
                        user_id: message.user_id,
                    }))
                    .await
                    .ok()?
                    .into_inner();

                self.cache.parse_and_update_user_info(user_info)?;
            }
            EventVariant::Deleted => {
                let uid = DieselUlid::from_str(&message.user_id).ok()?;
                self.cache.remove_all_tokens_by_user(uid);
                self.cache.remove_permission(uid, None, true)
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
                    match r.resource_variant() {
                        ResourceVariant::Project => {
                            let project_info = self
                                .project_service
                                .clone()
                                .as_mut()?
                                .get_project(Request::new(GetProjectRequest {
                                    project_id: r.resource_id,
                                }))
                                .await
                                .ok()?
                                .into_inner();
                            // Todo: Process project
                        }
                        ResourceVariant::Collection => {
                            let collection_info = self
                                .collection_service
                                .clone()
                                .as_mut()?
                                .get_collection(Request::new(GetCollectionRequest {
                                    collection_id: r.resource_id,
                                }))
                                .await
                                .ok()?
                                .into_inner();
                            // Todo: Process collection,
                        }
                        ResourceVariant::Dataset => {
                            let dataset_info = self
                                .dataset_service
                                .clone()
                                .as_mut()?
                                .get_dataset(Request::new(GetDatasetRequest {
                                    dataset_id: r.resource_id,
                                }))
                                .await
                                .ok()?
                                .into_inner();
                            // Todo: Process collection,
                        }
                        ResourceVariant::Object => {
                            let object_info = self
                                .object_service
                                .clone()
                                .as_mut()?
                                .get_object(Request::new(GetObjectRequest {
                                    object_id: r.resource_id,
                                }))
                                .await
                                .ok()?
                                .into_inner();
                            // Todo: Process collection,
                        }
                        _ => (),
                    }
                }
            }
            EventVariant::Deleted => {
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
}

#[cfg(test)]
mod tests {
    use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
    use aruna_rust_api::api::notification::services::v2::EventMessage;
    use aruna_rust_api::api::notification::services::v2::Reply;
    use aruna_rust_api::api::notification::services::v2::Resource as APIResource;
    use aruna_rust_api::api::notification::services::v2::ResourceEvent;

    fn mtemplate(res: APIResource) -> EventMessage {
        EventMessage {
            message_variant: Some(MessageVariant::ResourceEvent(ResourceEvent {
                resource: Some(res.clone()),
                event_variant: res.resource_variant,
                reply: Some(Reply {
                    reply: "a_reply".into(),
                    salt: "a_salt".into(),
                    hmac: "a_hmac".into(),
                }),
            })),
        }
    }
}
