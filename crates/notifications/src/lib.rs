use std::{marker::PhantomData, sync::Arc};

use cdl_dto::ingestion::OwnMessage;
use serde::Serialize;

use crate::full_notification_sender::{FullNotificationSender, FullNotificationSenderBase};

pub mod full_notification_sender;

/// This is convenience trait for types that do not implement Serialize (looking at you `tonic`),
/// but we'd prefer to avoid unnecessary cloning of their instances when using NotificationPublisher::Disabled.
pub trait IntoSerialize<S: Serialize> {
    fn into_serialize(self) -> S;
}

impl<S> IntoSerialize<S> for S
where
    S: Serialize,
{
    fn into_serialize(self) -> S {
        self
    }
}

#[derive(Clone)]
pub enum NotificationPublisher<T, S = T>
where
    T: IntoSerialize<S> + Send + Sync + 'static,
    S: Serialize,
{
    Full(FullNotificationSenderBase<T, S>),
    Disabled,
}

#[async_trait::async_trait]
pub trait NotificationService: Send + Sync + 'static {
    async fn notify(self: Arc<Self>, description: &str) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl NotificationService for () {
    async fn notify(self: Arc<Self>, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<T, S> NotificationPublisher<T, S>
where
    T: IntoSerialize<S> + Send + Sync + 'static + Clone,
    S: Serialize + Send + Sync + 'static,
{
    pub fn and_message_body<U>(&self, msg: &U) -> Arc<dyn NotificationService>
    where
        U: OwnMessage<Owned = T>,
    {
        match self {
            NotificationPublisher::Full(config) => Arc::new(FullNotificationSender {
                application: config.application,
                producer: config.publisher.clone(),
                destination: config.destination.clone(),
                context: config.context.clone(),
                msg: msg.to_owned_message(),
                _phantom: PhantomData,
            }),
            NotificationPublisher::Disabled => Arc::new(()),
        }
    }
}
