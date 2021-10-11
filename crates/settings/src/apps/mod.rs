use std::{env, future::Future};

use communication_utils::publisher::CommonPublisher;
use derive_more::Display;
use lapin::options::BasicConsumeOptions;
use notification_utils::{
    full_notification_sender::FullNotificationSenderBase,
    IntoSerialize,
    NotificationPublisher,
};
use rpc::schema_registry::types::SchemaType;
use serde::{Deserialize, Serialize};

pub mod api;
pub mod command_service;
pub mod data_router;
pub mod edge_registry;
pub mod materializer_general;
pub mod materializer_ondemand;
pub mod object_builder;
pub mod partial_update_engine;
pub mod query_router;
pub mod query_service;
pub mod query_service_ts;
pub mod schema_registry;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct AmqpConsumeOptions {
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub nowait: bool,
}

impl From<AmqpConsumeOptions> for BasicConsumeOptions {
    fn from(o: AmqpConsumeOptions) -> Self {
        BasicConsumeOptions {
            no_local: o.no_local,
            no_ack: o.no_ack,
            exclusive: o.exclusive,
            nowait: o.nowait,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MonitoringSettings {
    #[serde(default)]
    pub metrics_port: u16,
    #[serde(default = "default_status_port")]
    pub status_port: u16,
    #[serde(default = "default_otel_service_name")]
    pub otel_service_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LogSettings {
    pub rust_log: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum RepositoryType {
    #[serde(rename = "DocumentStorage")]
    Document,
    Timeseries,
}

impl From<RepositoryType> for SchemaType {
    fn from(typ: RepositoryType) -> SchemaType {
        match typ {
            RepositoryType::Document => SchemaType::DocumentStorage,
            RepositoryType::Timeseries => SchemaType::Timeseries,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostgresSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub dbname: String,
    pub schema: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CommunicationMethod {
    Kafka,
    Amqp,
    Grpc,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RepositoryStaticRouting {
    pub insert_destination: String,
    pub query_address: String,
    pub repository_type: RepositoryType,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NotificationSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub destination: String,
}

impl Default for LogSettings {
    fn default() -> Self {
        Self {
            rust_log: "info".to_string(),
        }
    }
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            username: "postgres".to_string(),
            password: "1234".to_string(),
            dbname: "postgres".to_string(),
            schema: "public".to_string(),
        }
    }
}

impl NotificationSettings {
    pub async fn publisher<T, S, F, Fut>(
        &self,
        publisher: F,
        context: String,
        application: &'static str,
    ) -> anyhow::Result<NotificationPublisher<T, S>>
    where
        T: IntoSerialize<S> + Send + Sync + 'static,
        S: Serialize,
        F: Fn() -> Fut,
        Fut: Future<Output = anyhow::Result<CommonPublisher>>,
    {
        Ok(if self.enabled {
            NotificationPublisher::Full(
                FullNotificationSenderBase::new(
                    publisher().await?,
                    self.destination.clone(),
                    context,
                    application,
                )
                .await,
            )
        } else {
            NotificationPublisher::Disabled
        })
    }
}

const fn default_status_port() -> u16 {
    3000
}

fn default_otel_service_name() -> String {
    env::current_exe()
        .expect("Current executable name")
        .to_string_lossy()
        .to_string()
}

pub(crate) const fn default_async_task_limit() -> usize {
    32
}
