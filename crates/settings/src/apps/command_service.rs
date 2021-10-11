use std::net::SocketAddrV4;

use anyhow::bail;
use communication_utils::{
    parallel_consumer::{ParallelCommonConsumer, ParallelCommonConsumerConfig},
    publisher::CommonPublisher,
};
use lapin::options::BasicConsumeOptions;
use serde::{Deserialize, Serialize};
use task_utils::task_limiter::TaskLimiter;
use url::Url;

use crate::{
    apps::{
        default_async_task_limit,
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
pub struct CommandServiceSettings {
    pub communication_method: CommunicationMethod,
    pub repository_kind: CommandServiceRepositoryKind,

    #[serde(default = "default_async_task_limit")]
    pub async_task_limit: usize,

    pub postgres: Option<PostgresSettings>,
    pub victoria_metrics: Option<CommandServiceVictoriaMetricsSettings>,
    pub druid: Option<CommandServiceDruidSettings>,

    pub kafka: Option<CommandServiceKafkaSettings>,
    pub amqp: Option<CommandServiceAmqpSettings>,
    pub grpc: Option<CommandServiceGRpcSettings>,

    #[serde(default)]
    pub notifications: NotificationSettings,

    pub listener: CommandServiceListenerSettings,

    pub monitoring: MonitoringSettings,

    #[serde(default)]
    pub log: LogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandServiceRepositoryKind {
    Postgres,
    Druid,
    VictoriaMetrics,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandServiceVictoriaMetricsSettings {
    pub url: Url,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandServiceDruidSettings {
    pub topic: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandServiceKafkaSettings {
    pub brokers: String,
    pub group_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandServiceAmqpSettings {
    pub exchange_url: String,
    pub tag: String,
    pub consume_options: Option<AmqpConsumeOptions>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandServiceGRpcSettings {
    pub address: SocketAddrV4,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommandServiceListenerSettings {
    #[serde(default)]
    pub ordered_sources: String,
    #[serde(default)]
    pub unordered_sources: String,
}

impl CommandServiceSettings {
    pub async fn publisher(&self) -> anyhow::Result<CommonPublisher> {
        publisher(
            self.kafka.as_ref().map(|kafka| kafka.brokers.as_str()),
            self.amqp.as_ref().map(|amqp| amqp.exchange_url.as_str()),
            self.grpc.as_ref().map(|_| ()),
        )
        .await
    }

    pub async fn consumers(&self) -> anyhow::Result<Vec<ParallelCommonConsumer>> {
        let task_limiter = TaskLimiter::new(self.async_task_limit);

        match (&self.kafka, &self.amqp, &self.grpc) {
            (Some(kafka), _, _) if self.communication_method == CommunicationMethod::Kafka => {
                if self.listener.is_empty() {
                    bail!("Missing list of listener queues")
                }

                kafka
                    .parallel_consumers(
                        self.listener
                            .ordered_sources
                            .split(',')
                            .filter(|s| !s.is_empty()),
                        self.listener
                            .unordered_sources
                            .split(',')
                            .filter(|s| !s.is_empty()),
                        task_limiter,
                    )
                    .await
            }
            (_, Some(amqp), _) if self.communication_method == CommunicationMethod::Amqp => {
                if self.listener.is_empty() {
                    bail!("Missing list of listener queues")
                }

                amqp.parallel_consumers(
                    self.listener
                        .ordered_sources
                        .split(',')
                        .filter(|s| !s.is_empty()),
                    self.listener
                        .unordered_sources
                        .split(',')
                        .filter(|s| !s.is_empty()),
                    task_limiter,
                )
                .await
            }
            (_, _, Some(grpc)) if self.communication_method == CommunicationMethod::Grpc => {
                Ok(vec![grpc.parallel_consumer().await?])
            }
            _ => anyhow::bail!("Unsupported consumer specification"),
        }
    }
}

impl CommandServiceListenerSettings {
    pub fn is_empty(&self) -> bool {
        self.ordered_sources.is_empty() && self.unordered_sources.is_empty()
    }
}

impl CommandServiceKafkaSettings {
    pub async fn parallel_consumers<'a>(
        &self,
        ordered_sources: impl Iterator<Item = &'a str>,
        unordered_sources: impl Iterator<Item = &'a str>,
        task_limiter: TaskLimiter,
    ) -> anyhow::Result<Vec<ParallelCommonConsumer>> {
        let mut result = Vec::new();

        for topic in ordered_sources.chain(unordered_sources) {
            result.push(
                ParallelCommonConsumer::new(ParallelCommonConsumerConfig::Kafka {
                    brokers: &self.brokers,
                    group_id: &self.group_id,
                    topic,
                    task_limiter: task_limiter.clone(),
                })
                .await?,
            )
        }

        Ok(result)
    }
}

impl CommandServiceAmqpSettings {
    pub async fn parallel_consumers<'a>(
        &self,
        ordered_sources: impl Iterator<Item = &'a str>,
        unordered_sources: impl Iterator<Item = &'a str>,
        task_limiter: TaskLimiter,
    ) -> anyhow::Result<Vec<ParallelCommonConsumer>> {
        let mut result = Vec::new();

        for queue_name in ordered_sources {
            result.push(
                ParallelCommonConsumer::new(ParallelCommonConsumerConfig::Amqp {
                    connection_string: &self.exchange_url.clone(),
                    consumer_tag: &self.tag.clone(),
                    queue_name,
                    task_limiter: task_limiter.clone(),
                    options: self.consume_options.map(|o| {
                        let mut bco: BasicConsumeOptions = o.into();
                        bco.exclusive = true;
                        bco
                    }),
                })
                .await?,
            )
        }

        for queue_name in unordered_sources {
            result.push(
                ParallelCommonConsumer::new(ParallelCommonConsumerConfig::Amqp {
                    connection_string: &self.exchange_url.clone(),
                    consumer_tag: &self.tag.clone(),
                    queue_name,
                    task_limiter: task_limiter.clone(),
                    options: self.consume_options.map(|o| {
                        let mut bco: BasicConsumeOptions = o.into();
                        bco.exclusive = false;
                        bco
                    }),
                })
                .await?,
            )
        }

        Ok(result)
    }
}

impl CommandServiceGRpcSettings {
    pub async fn parallel_consumer(&self) -> anyhow::Result<ParallelCommonConsumer> {
        Ok(
            ParallelCommonConsumer::new(ParallelCommonConsumerConfig::Grpc { addr: self.address })
                .await?,
        )
    }
}
