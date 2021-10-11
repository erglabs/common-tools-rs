use communication_utils::{
    consumer::{CommonConsumer, CommonConsumerConfig},
    publisher::CommonPublisher,
};
use serde::{Deserialize, Serialize};

use crate::{
    apps::{
        AmqpConsumeOptions,
        CommunicationMethod,
        LogSettings,
        MonitoringSettings,
        NotificationSettings,
        PostgresSettings,
    },
    publisher,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EdgeRegistrySettings {
    pub communication_method: CommunicationMethod,
    pub input_port: u16,

    pub postgres: PostgresSettings,

    pub kafka: Option<EdgeRegistryKafkaSettings>,
    pub amqp: Option<EdgeRegistryAmqpSettings>,

    #[serde(default)]
    pub notifications: NotificationSettings,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EdgeRegistryKafkaSettings {
    pub brokers: String,
    pub group_id: String,
    pub ingest_topic: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EdgeRegistryAmqpSettings {
    pub exchange_url: String,
    pub tag: String,
    pub ingest_queue: String,
    pub consume_options: Option<AmqpConsumeOptions>,
}

impl EdgeRegistrySettings {
    pub async fn publisher(&self) -> anyhow::Result<CommonPublisher> {
        publisher(
            self.kafka.as_ref().map(|kafka| kafka.brokers.as_str()),
            self.amqp.as_ref().map(|amqp| amqp.exchange_url.as_str()),
            None,
        )
        .await
    }
}

impl EdgeRegistryKafkaSettings {
    pub async fn consumer(&self) -> anyhow::Result<CommonConsumer> {
        Ok(CommonConsumer::new(CommonConsumerConfig::Kafka {
            brokers: &self.brokers,
            group_id: &self.group_id,
            topic: self.ingest_topic.as_str(),
        })
        .await?)
    }
}

impl EdgeRegistryAmqpSettings {
    pub async fn consumer(&self) -> anyhow::Result<CommonConsumer> {
        Ok(CommonConsumer::new(CommonConsumerConfig::Amqp {
            connection_string: &self.exchange_url,
            consumer_tag: &self.tag,
            queue_name: &self.ingest_queue,
            options: self.consume_options.map(|o| o.into()),
        })
        .await?)
    }
}
