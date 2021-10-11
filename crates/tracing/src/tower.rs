#![allow(clippy::type_complexity)]
use std::pin::Pin;

use futures_util::Future;
use http::{Request, Response};
use http_body::Body;
use opentelemetry::global;
use tower::{Layer, Service};
use tower_http::classify::{ClassifiedResponse, ClassifyResponse, GrpcErrorsAsFailures};
use tracing_futures::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Clone)]
pub struct TraceLayer;

impl<S> Layer<S> for TraceLayer {
    type Service = crate::tower::Trace<S>;

    fn layer(&self, inner: S) -> Self::Service {
        crate::tower::Trace::new(inner)
    }
}

/// Service which injects span based on HTTP header
/// # Example (gRPC)
/// ```ignore
///    let conn = tower::ServiceBuilder::new()
///        .layer_fn(Trace::new)
///        .service(conn);
///
///    Ok(MyServiceClient::new(conn))
/// ```
#[derive(Debug)]
pub struct Trace<S> {
    pub(crate) inner: S,
}

impl<S: Clone> Clone for Trace<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> Trace<S> {
    pub(crate) fn new(inner: S) -> Self {
        Trace { inner }
    }
}

impl<S, ReqBody, ResBody> tower::Service<Request<ReqBody>> for Trace<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ReqBody: Body + std::fmt::Debug,
    ResBody: Body,
    ResBody::Error: std::fmt::Display + 'static,
    S::Error: std::fmt::Display + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        let span = tracing::info_span!(
            "request",
            method = %req.method(),
            uri = %req.uri(),
            version = ?req.version(),
            headers = ?req.headers()
        );
        let parent_cx = global::get_text_map_propagator(|prop| {
            prop.extract(&opentelemetry_http::HeaderExtractor(req.headers()))
        });
        span.set_parent(parent_cx);

        global::get_text_map_propagator(|prop| {
            prop.inject_context(
                &tracing::Span::current().context(),
                &mut opentelemetry_http::HeaderInjector(req.headers_mut()),
            )
        });

        let classifier = GrpcErrorsAsFailures::new();

        let fut = self.inner.call(req).instrument(span);

        Box::pin(async move {
            let response = fut.await?;
            if let ClassifiedResponse::Ready(Err(error)) = classifier.classify_response(&response) {
                tracing::error!(%error, "Response error");
            }
            Ok(response)
        })
    }
}
