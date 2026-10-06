// SPDX-License-Identifier: MPL-2.0
mod common;

use common::{publish, read_frame, subscribe, wait_until, TestServer};
use mqtt_tcp_server::ServerConfig;
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

const GET: &str = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";

fn samples(response: &str) -> BTreeMap<&str, u64> {
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("content-type: text/plain; version=0.0.4; charset=utf-8\r\n"));
    let body = response.split_once("\r\n\r\n").unwrap().1;
    assert!(body.ends_with('\n'));
    let result: BTreeMap<_, _> = body
        .lines()
        .filter(|l| !l.starts_with('#'))
        .map(|l| {
            let (name, value) = l.split_once(' ').unwrap();
            (name, value.parse().unwrap())
        })
        .collect();
    assert_eq!(result.len(), 8);
    for name in result.keys() {
        let kind = if ["sessions", "connections", "subs"].contains(name) {
            "gauge"
        } else {
            "counter"
        };
        assert_eq!(body.matches(&format!("# TYPE {name} {kind}\n")).count(), 1);
        assert_eq!(body.matches(&format!("# HELP {name} ")).count(), 1);
    }
    result
}

#[tokio::test]
async fn scrape_counts_mqtt_traffic_without_counting_itself_or_resetting() {
    let server = TestServer::start(ServerConfig::default()).await;
    let initial = server.scrape(GET).await;
    assert!(samples(&initial).values().all(|v| *v == 0));
    let mut socket = server.client(5).await;
    let input = [
        subscribe(5, &[0, 1, 2]),
        publish(5, 0, 0, false),
        publish(5, 1, 9, false),
        publish(5, 2, 10, false),
        publish(5, 2, 10, true),
        vec![0x62, 2, 0, 10, 0xc0, 0],
    ]
    .concat();
    socket.write_all(&input).await.unwrap();
    for _ in 0..6 {
        read_frame(&mut socket).await;
    }
    let response = server.scrape(GET).await;
    let before = samples(&response);
    assert_eq!(
        before,
        BTreeMap::from([
            ("sessions", 0),
            ("connections", 1),
            ("subs", 0),
            ("conn_rcvd", 1),
            ("pub_rcvd", 4),
            ("sub_rcvd", 1),
            ("pub_qos0_sent", 0),
            ("pub_qos1_sent", 0),
        ])
    );
    assert_eq!(samples(&server.scrape(GET).await), before);
    drop(socket);
    wait_until(|| server.metrics.snapshot().connections == 0).await;
    let after_close = server.scrape(GET).await;
    let mut expected = before.clone();
    expected.insert("connections", 0);
    assert_eq!(samples(&after_close), expected);
    let socket = server.client(5).await; // Fake recovery of same ClientID.
    let reconnect = server.scrape(GET).await;
    assert_eq!(samples(&reconnect)["sessions"], 0);
    assert_eq!(samples(&reconnect)["conn_rcvd"], 2);
    drop(socket);
    server.shutdown().await;
}

#[tokio::test]
async fn routes_methods_and_pending_mqtt_connections_have_correct_metrics() {
    let server = TestServer::start(ServerConfig::default()).await;
    assert!(server
        .scrape("GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .starts_with("HTTP/1.1 404"));
    let method = server
        .scrape("POST /metrics HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
        .await;
    assert!(method.starts_with("HTTP/1.1 405"));
    assert!(method.contains("allow: GET\r\n"));
    let socket = TcpStream::connect(server.mqtt_addr).await.unwrap();
    wait_until(|| server.metrics.snapshot().connections == 1).await;
    let response = server.scrape(GET).await;
    assert_eq!(samples(&response)["connections"], 1);
    assert_eq!(samples(&response)["conn_rcvd"], 0);
    drop(socket);
    server.shutdown().await;
}

#[tokio::test]
async fn slow_and_oversized_scrapes_do_not_block_mqtt_or_shutdown() {
    let server = TestServer::start(ServerConfig {
        max_scrapes: 1,
        metrics_timeout: Duration::from_millis(150),
        ..Default::default()
    })
    .await;
    let mut slow = TcpStream::connect(server.metrics_addr.unwrap())
        .await
        .unwrap();
    slow.write_all(b"GET /metrics HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    let mut mqtt = server.client(5).await;
    mqtt.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(read_frame(&mut mqtt).await, vec![0xd0, 0]);
    let mut closed = Vec::new();
    timeout(Duration::from_secs(2), slow.read_to_end(&mut closed))
        .await
        .unwrap()
        .unwrap();
    let oversized = format!(
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nX-Large: {}\r\n\r\n",
        "x".repeat(9000)
    );
    let mut oversized_socket = TcpStream::connect(server.metrics_addr.unwrap())
        .await
        .unwrap();
    oversized_socket
        .write_all(oversized.as_bytes())
        .await
        .unwrap();
    let mut rejected = String::new();
    let read = timeout(
        Duration::from_secs(2),
        oversized_socket.read_to_string(&mut rejected),
    )
    .await
    .unwrap();
    // Closing with unread oversized headers may generate an RST on TCP.
    if let Err(error) = read {
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }
    assert!(rejected.is_empty() || rejected.starts_with("HTTP/1.1 431"));
    assert_eq!(samples(&server.scrape(GET).await)["connections"], 1);
    let pending = TcpStream::connect(server.metrics_addr.unwrap())
        .await
        .unwrap();
    server.shutdown().await;
    drop((mqtt, pending));
}

#[tokio::test]
async fn tiny_output_batches_preserve_all_received_totals() {
    let mut config = ServerConfig::default();
    config.engine.max_output_bytes = 5;
    config.engine.max_packets_per_poll = 2;
    let server = TestServer::start(config).await;
    let mut socket = server.client(5).await;
    let mut input = Vec::new();
    for id in 1..=100 {
        input.extend(publish(5, 1, id, false));
    }
    socket.write_all(&input).await.unwrap();
    for id in 1u16..=100 {
        assert_eq!(
            read_frame(&mut socket).await,
            [vec![0x40, 2], id.to_be_bytes().to_vec()].concat()
        );
    }
    assert_eq!(samples(&server.scrape(GET).await)["pub_rcvd"], 100);
    drop(socket);
    wait_until(|| server.metrics.snapshot().connections == 0).await;
    assert_eq!(samples(&server.scrape(GET).await)["pub_rcvd"], 100);
    server.shutdown().await;
}
