use std::path::PathBuf;

use communication_utils::metadata_fetcher::MetadataFetcher;
use serde::{Deserialize, Serialize};

use crate::apps::{CommunicationMethod, LogSettings, MonitoringSettings, PostgresSettings};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaRegistrySettings {
    pub communication_method: CommunicationMethod,
    pub input_port: u16,
    pub import_file: Option<PathBuf>,
    pub export_dir: Option<PathBuf>,

    pub services: SchemaRegistryServicesSettings,

    pub postgres: PostgresSettings,

    pub kafka: Option<SchemaRegistryKafkaSettings>,
    pub amqp: Option<SchemaRegistryAmqpSettings>,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaRegistryServicesSettings {
    pub edge_registry_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaRegistryKafkaSettings {
    pub brokers: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaRegistryAmqpSettings {
    pub exchange_url: String,
}

impl SchemaRegistrySettings {
    pub async fn metadata_fetcher(&self) -> anyhow::Result<MetadataFetcher> {
        Ok(match (&self.kafka, &self.amqp, self.communication_method) {
            (Some(kafka), _, CommunicationMethod::Kafka) => {
                MetadataFetcher::new_kafka(kafka.brokers.as_str()).await?
            }
            (_, Some(amqp), CommunicationMethod::Amqp) => {
                MetadataFetcher::new_amqp(amqp.exchange_url.as_str()).await?
            }
            (_, _, CommunicationMethod::Grpc) => MetadataFetcher::new_grpc()?,
            _ => anyhow::bail!("Unsupported consumer specification"),
        })
    }
}
