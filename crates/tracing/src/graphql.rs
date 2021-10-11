use async_graphql::ErrorExtensionValues;
use opentelemetry::{global, propagation::Injector};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Method used with async_graphql::Error inject Span ID
pub fn inject_span<E>(_e: E, error: &mut ErrorExtensionValues) {
    let mut error = ErrorExtensions::new(error);
    global::get_text_map_propagator(|prop| {
        prop.inject_context(&tracing::Span::current().context(), &mut error);
    });
}

struct ErrorExtensions<'a> {
    values: &'a mut ErrorExtensionValues,
}

impl<'a> ErrorExtensions<'a> {
    fn new(values: &'a mut ErrorExtensionValues) -> Self {
        Self { values }
    }
}

impl<'a> Injector for ErrorExtensions<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.values.set(key, value);
    }
}
