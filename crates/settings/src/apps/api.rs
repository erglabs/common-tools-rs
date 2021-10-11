use serde::{Deserialize, Serialize};

use crate::apps::{CommunicationMethod, LogSettings};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiSettings {
    pub communication_method: CommunicationMethod,
    pub input_port: u16,
    pub insert_destination: String,

    pub kafka: Option<ApiKafkaSettings>,
    pub amqp: Option<ApiAmqpSettings>,

    pub services: ApiServiceSettings,

    pub notification_consumer: Option<ApiNotificationConsumerSettings>,

    #[serde(default)]
    pub log: LogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiKafkaSettings {
    pub brokers: String,
    pub group_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiAmqpSettings {
    pub exchange_url: String,
    pub tag: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiServiceSettings {
    pub schema_registry_url: String,
    pub edge_registry_url: String,
    pub on_demand_materializer_url: String,
    pub query_router_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiNotificationConsumerSettings {
    pub source: String,
}
