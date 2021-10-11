use serde::{Deserialize, Serialize};

use crate::apps::{CommunicationMethod, LogSettings, MonitoringSettings};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartialUpdateEngineSettings {
    pub communication_method: CommunicationMethod,
    pub sleep_phase_length: u64,

    pub kafka: PartialUpdateEngineKafkaSettings,
    pub notification_consumer: PartialUpdateEngineNotificationConsumerSettings,
    pub services: PartialUpdateEngineServicesSettings,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartialUpdateEngineKafkaSettings {
    pub brokers: String,
    pub egest_topic: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartialUpdateEngineNotificationConsumerSettings {
    pub brokers: String,
    pub group_id: String,
    pub source: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PartialUpdateEngineServicesSettings {
    pub schema_registry_url: String,
}
