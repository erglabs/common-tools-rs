use std::{collections::HashMap, net::SocketAddrV4};

use communication_utils::{
    parallel_consumer::{ParallelCommonConsumer, ParallelCommonConsumerConfig},
    publisher::CommonPublisher,
};
use serde::{Deserialize, Serialize};
use task_utils::task_limiter::TaskLimiter;

use crate::apps::{
    default_async_task_limit,
    AmqpConsumeOptions,
    CommunicationMethod,
    LogSettings,
    MonitoringSettings,
    RepositoryStaticRouting,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataRouterSettings {
    pub communication_method: CommunicationMethod,
    pub cache_capacity: usize,
    #[serde(default = "default_async_task_limit")]
    pub async_task_limit: usize,

    pub kafka: Option<DataRouterConsumerKafkaSettings>,
    pub amqp: Option<DataRouterAmqpSettings>,
    pub grpc: Option<DataRouterGRpcSettings>,

    pub monitoring: MonitoringSettings,

    pub services: DataRouterServicesSettings,

    #[serde(default)]
    pub log: LogSettings,

    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub repositories: HashMap<String, RepositoryStaticRouting>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataRouterConsumerKafkaSettings {
    pub brokers: String,
    pub group_id: String,
    pub ingest_topic: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataRouterAmqpSettings {
    pub exchange_url: String,
    pub tag: String,
    pub ingest_queue: String,
    pub consume_options: Option<AmqpConsumeOptions>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataRouterGRpcSettings {
    pub address: SocketAddrV4,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataRouterServicesSettings {
    pub schema_registry_url: String,
}

impl DataRouterSettings {
    pub async fn consumer(&self) -> anyhow::Result<ParallelCommonConsumer> {
        match (
            &self.kafka,
            &self.amqp,
            &self.grpc,
            &self.communication_method,
        ) {
            (Some(kafka), _, _, CommunicationMethod::Kafka) => {
                kafka
                    .parallel_consumer(TaskLimiter::new(self.async_task_limit))
                    .await
            }
            (_, Some(amqp), _, CommunicationMethod::Amqp) => {
                amqp.parallel_consumer(TaskLimiter::new(self.async_task_limit))
                    .await
            }
            (_, _, Some(grpc), CommunicationMethod::Grpc) => grpc.parallel_consumer().await,
            _ => anyhow::bail!("Unsupported consumer specification"),
        }
    }

    pub async fn producer(&self) -> anyhow::Result<CommonPublisher> {
        Ok(
            match (
                &self.kafka,
                &self.amqp,
                &self.grpc,
                &self.communication_method,
            ) {
                (Some(kafka), _, _, CommunicationMethod::Kafka) => {
                    CommonPublisher::new_kafka(&kafka.brokers).await?
                }
                (_, Some(amqp), _, CommunicationMethod::Amqp) => {
                    CommonPublisher::new_amqp(&amqp.exchange_url).await?
                }
                (_, _, Some(_), CommunicationMethod::Grpc) => CommonPublisher::new_grpc().await?,
                _ => anyhow::bail!("Unsupported consumer specification"),
            },
        )
    }
}

impl DataRouterConsumerKafkaSettings {
    pub async fn parallel_consumer(
        &self,
        task_limiter: TaskLimiter,
    ) -> anyhow::Result<ParallelCommonConsumer> {
        Ok(
            ParallelCommonConsumer::new(ParallelCommonConsumerConfig::Kafka {
                brokers: &self.brokers,
                group_id: &self.group_id,
                topic: &self.ingest_topic,
                task_limiter,
            })
            .await?,
        )
    }
}

impl DataRouterAmqpSettings {
    pub async fn parallel_consumer(
        &self,
        task_limiter: TaskLimiter,
    ) -> anyhow::Result<ParallelCommonConsumer> {
        Ok(
            ParallelCommonConsumer::new(ParallelCommonConsumerConfig::Amqp {
                connection_string: &self.exchange_url,
                consumer_tag: &self.tag,
                queue_name: &self.ingest_queue,
                options: self.consume_options.map(|o| o.into()),
                task_limiter,
            })
            .await?,
        )
    }
}

impl DataRouterGRpcSettings {
    pub async fn parallel_consumer(&self) -> anyhow::Result<ParallelCommonConsumer> {
        Ok(
            ParallelCommonConsumer::new(ParallelCommonConsumerConfig::Grpc { addr: self.address })
                .await?,
        )
    }
}
