// SPDX-License-Identifier: MPL-2.0

//! Bounded HTTP/1 scrape handling, independent of MQTT engines and I/O locks.

use crate::BrokerMetrics;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    body::Incoming,
    header::{ALLOW, CONTENT_TYPE},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::watch, time::timeout};

pub const CONTENT_TYPE_VALUE: &str = "text/plain; version=0.0.4; charset=utf-8";

async fn respond(
    req: Request<Incoming>,
    metrics: Arc<BrokerMetrics>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (status, body, content_type) = if req.uri().path() != "/metrics" {
        (
            StatusCode::NOT_FOUND,
            "not found\n".to_owned(),
            "text/plain; charset=utf-8",
        )
    } else if req.method() != Method::GET {
        (
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed\n".to_owned(),
            "text/plain; charset=utf-8",
        )
    } else {
        (StatusCode::OK, metrics.render(), CONTENT_TYPE_VALUE)
    };
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, content_type.parse().unwrap());
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response.headers_mut().insert(ALLOW, "GET".parse().unwrap());
    }
    Ok(response)
}

pub(crate) async fn serve_scrape(
    socket: TcpStream,
    metrics: Arc<BrokerMetrics>,
    deadline: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let service = service_fn(move |req| respond(req, Arc::clone(&metrics)));
    let mut http = http1::Builder::new();
    http.keep_alive(false)
        .max_buf_size(8192)
        .timer(TokioTimer::new())
        .header_read_timeout(deadline);
    let connection = http.serve_connection(TokioIo::new(socket), service);
    tokio::select! {
        _ = stop.changed() => {},
        // Covers the complete request/response, including slow writers.
        _ = timeout(deadline, connection) => {},
    }
}
