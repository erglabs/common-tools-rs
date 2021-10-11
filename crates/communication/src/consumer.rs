#![allow(unused_imports, unused_variables)]
use anyhow::Context;
use async_trait::async_trait;
use futures_util::TryStreamExt;
#[cfg(feature = "amqp")]
pub use lapin::options::BasicConsumeOptions;
#[cfg(feature = "amqp")]
use lapin::types::FieldTable;
#[cfg(feature = "kafka")]
use rdkafka::{
    consumer::{DefaultConsumerContext, StreamConsumer},
    ClientConfig,
};
#[cfg(feature = "amqp")]
use tokio_amqp::LapinTokioExt;
use tracing_futures::Instrument;

#[cfg(feature = "kafka")]
use super::kafka_ack_queue::KafkaAckQueue;
use super::{message::CommunicationMessage, Result};
#[cfg(feature = "amqp")]
use crate::message::AmqpCommunicationMessage;
#[cfg(feature = "kafka")]
use crate::message::KafkaCommunicationMessage;

#[async_trait]
pub trait ConsumerHandler {
    async fn handle<'a>(&'a mut self, msg: &'a dyn CommunicationMessage) -> anyhow::Result<()>;
}

pub enum CommonConsumerConfig<'a> {
    #[cfg(feature = "kafka")]
    Kafka {
        brokers: &'a str,
        group_id: &'a str,
        topic: &'a str,
    },
    #[cfg(feature = "amqp")]
    Amqp {
        connection_string: &'a str,
        consumer_tag: &'a str,
        queue_name: &'a str,
        options: Option<BasicConsumeOptions>,
    },
}

pub enum CommonConsumer {
    #[cfg(feature = "kafka")]
    Kafka {
        consumer: StreamConsumer<DefaultConsumerContext>,
        ack_queue: KafkaAckQueue,
    },
    #[cfg(feature = "amqp")]
    Amqp { consumer: lapin::Consumer },
}
impl CommonConsumer {
    pub async fn new(config: CommonConsumerConfig<'_>) -> Result<Self> {
        match config {
            #[cfg(feature = "kafka")]
            CommonConsumerConfig::Kafka {
                group_id,
                brokers,
                topic,
            } => Self::new_kafka(group_id, brokers, &[topic]).await,
            #[cfg(feature = "amqp")]
            CommonConsumerConfig::Amqp {
                connection_string,
                consumer_tag,
                queue_name,
                options,
            } => Self::new_amqp(connection_string, consumer_tag, queue_name, options).await,
        }
    }

    #[cfg(feature = "kafka")]
    async fn new_kafka(group_id: &str, brokers: &str, topics: &[&str]) -> Result<Self> {
        let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true")
            .create()
            .context("Consumer creation failed")?;

        rdkafka::consumer::Consumer::subscribe(&consumer, topics)
            .context("Can't subscribe to specified topics")?;

        Ok(CommonConsumer::Kafka {
            consumer,
            ack_queue: Default::default(),
        })
    }

    #[cfg(feature = "amqp")]
    async fn new_amqp(
        connection_string: &str,
        consumer_tag: &str,
        queue_name: &str,
        consume_options: Option<BasicConsumeOptions>,
    ) -> Result<Self> {
        let consume_options = consume_options.unwrap_or_default();
        let connection = lapin::Connection::connect(
            connection_string,
            lapin::ConnectionProperties::default().with_tokio(),
        )
        .await?;
        let channel = connection.create_channel().await?;
        let consumer = channel
            .basic_consume(
                queue_name,
                consumer_tag,
                consume_options,
                FieldTable::default(),
            )
            .await?;
        Ok(CommonConsumer::Amqp { consumer })
    }

    /// Process messages in order. Cannot be used with Grpc.
    /// # Error handling
    /// Function returns and error on first unhandled message.
    #[allow(unused_mut)]
    pub async fn run(self, mut handler: impl ConsumerHandler) -> Result<()> {
        match self {
            #[cfg(feature = "kafka")]
            CommonConsumer::Kafka {
                consumer,
                ack_queue,
            } => {
                let mut message_stream = consumer.stream();
                while let Some(message) = message_stream.try_next().await? {
                    ack_queue.add(&message);
                    let span = tracing::info_span!("consume_message");
                    let result = async {
                        tracing_utils::kafka::set_parent_span(&message);
                        let message = KafkaCommunicationMessage { message };
                        handler.handle(&message).await.map(|_| {
                            ack_queue.ack(&message.message, &consumer);
                        })
                    }
                    .instrument(span)
                    .await;
                    if let Err(e) = result {
                        tracing::error!(?e, "Couldn't process message");
                    }
                }
            }
            #[cfg(feature = "amqp")]
            CommonConsumer::Amqp { mut consumer } => {
                while let Some((channel, delivery)) = consumer.try_next().await? {
                    let message = AmqpCommunicationMessage { delivery };
                    match handler.handle(&message).await {
                        Ok(_) => {
                            channel
                                .basic_ack(message.delivery.delivery_tag, Default::default())
                                .await?;
                        }
                        Err(e) => {
                            tracing::error!(?e, "Couldn't process message");
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
