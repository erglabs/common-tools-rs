use std::convert::Infallible;

use futures_util::TryFuture;
use hyper::Request;
use opentelemetry::global;
use opentelemetry_http::HeaderInjector;
use reqwest::RequestBuilder;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Wrapper for `warp::serve` used to set Span ID propagated to HTTP Request to every defined route.
pub async fn serve<R>(routes: R, addr: ([u8; 4], u16))
where
    R: warp::Filter + Clone + Send + Sync + 'static,
    <R::Future as TryFuture>::Ok: warp::Reply,
{
    let service = warp::service(routes);

    let make_svc = hyper::service::make_service_fn(move |_| {
        let service = service.clone();
        async move {
            let service = tower::ServiceBuilder::new()
                .layer(crate::tower::TraceLayer)
                .service(service);

            Ok::<_, Infallible>(service)
        }
    });

    hyper::Server::bind(&addr.into())
        .serve(make_svc)
        .await
        .unwrap();
}

/// Extension trait used with reqwest::Client to inject Span ID to HTTP Headers
pub trait RequestBuilderTracingExt {
    /// Injects SpanID to HTTP Headers
    fn inject_span(self) -> Self;
}

impl RequestBuilderTracingExt for RequestBuilder {
    fn inject_span(self) -> Self {
        let mut inner = Request::new(());
        global::get_text_map_propagator(|prop| {
            prop.inject_context(
                &tracing::Span::current().context(),
                &mut HeaderInjector(&mut inner.headers_mut()),
            )
        });
        let headers = inner.headers().clone();
        self.headers(headers)
    }
}
