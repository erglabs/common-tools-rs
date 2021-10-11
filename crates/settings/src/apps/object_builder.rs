use communication_utils::{
    consumer::{CommonConsumer, CommonConsumerConfig},
    publisher::CommonPublisher,
};
use serde::{Deserialize, Serialize};

use crate::apps::{
    AmqpConsumeOptions,
    CommunicationMethod,
    LogSettings,
    MonitoringSettings,
    NotificationSettings,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct ObjectBuilderSettings {
    pub communication_method: CommunicationMethod,
    pub input_port: u16,
    pub chunk_capacity: usize,

    pub kafka: Option<ObjectBuilderKafkaSettings>,
    pub amqp: Option<ObjectBuilderAmqpSettings>,

    pub services: ObjectBuilderServicesSettings,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,

    #[serde(default)]
    pub notifications: NotificationSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectBuilderKafkaSettings {
    pub brokers: String,
    pub group_id: String,
    pub ingest_topic: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectBuilderAmqpSettings {
    pub exchange_url: String,
    pub tag: String,
    pub ingest_queue: String,
    pub consume_options: Option<AmqpConsumeOptions>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ObjectBuilderServicesSettings {
    pub schema_registry_url: String,
    pub edge_registry_url: String,
}

impl ObjectBuilderSettings {
    pub async fn consumer(&self) -> anyhow::Result<CommonConsumer> {
        match (&self.kafka, &self.amqp, &self.communication_method) {
            (Some(kafka), _, CommunicationMethod::Kafka) => {
                Ok(CommonConsumer::new(CommonConsumerConfig::Kafka {
                    brokers: &kafka.brokers,
                    group_id: &kafka.group_id,
                    topic: &kafka.ingest_topic,
                })
                .await?)
            }
            (_, Some(amqp), CommunicationMethod::Amqp) => {
                Ok(CommonConsumer::new(CommonConsumerConfig::Amqp {
                    connection_string: &amqp.exchange_url,
                    consumer_tag: &amqp.tag,
                    queue_name: &amqp.ingest_queue,
                    options: amqp.consume_options.map(|o| o.into()),
                })
                .await?)
            }
            _ => anyhow::bail!("Unsupported consumer specification"),
        }
    }

    pub async fn publisher(&self) -> anyhow::Result<CommonPublisher> {
        match (&self.kafka, &self.amqp, self.communication_method) {
            (Some(kafka), _, CommunicationMethod::Kafka) => {
                Ok(CommonPublisher::new_kafka(&kafka.brokers).await?)
            }
            (_, Some(amqp), CommunicationMethod::Amqp) => {
                Ok(CommonPublisher::new_amqp(&amqp.exchange_url).await?)
            }
            _ => anyhow::bail!("Unsupported producer specification"),
        }
    }
}
