#![feature(trait_alias)]

use opentelemetry::{global, sdk::propagation::TraceContextPropagator};
use tokio::runtime::Handle;
use tracing_subscriber::{prelude::*, EnvFilter};

#[cfg(feature = "with_graphql")]
pub mod graphql;
#[cfg(feature = "with_grpc")]
pub mod grpc;
#[cfg(feature = "with_http")]
pub mod http;
#[cfg(feature = "with_kafka")]
pub mod kafka;
#[cfg(feature = "with_tower")]
pub mod tower;

pub fn init<'a>(
    rust_log: impl Into<Option<&'a str>>,
    otel_service_name: &str,
) -> anyhow::Result<()> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let opentelemetry = Handle::try_current()
        .ok() // Check if Tokio runtime exists
        .and_then(|_| {
            opentelemetry_jaeger::new_pipeline()
                .with_service_name(otel_service_name)
                .install_batch(opentelemetry::runtime::Tokio)
                .ok()
        })
        .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer));

    let fmt = tracing_subscriber::fmt::layer();

    let filter = rust_log
        .into()
        .map(EnvFilter::new)
        .unwrap_or_else(EnvFilter::from_default_env);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(opentelemetry)
        .try_init()?;

    Ok(())
}
