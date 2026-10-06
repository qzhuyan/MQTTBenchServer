// SPDX-License-Identifier: MPL-2.0
mod common;

use common::{connect, publish, read_frame, subscribe, wait_until, TestServer};
use mqtt_tcp_server::{ServerConfig, TcpServer};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::timeout,
};

#[tokio::test]
async fn concurrent_clients_pipeline_all_qos_and_cleanup() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut clients = JoinSet::new();
    for index in 0..24 {
        let addr = server.mqtt_addr;
        clients.spawn(async move {
            let version = if index % 2 == 0 { 4 } else { 5 };
            let mut socket = TcpStream::connect(addr).await.unwrap();
            let first = connect(version, false, b"same-id-no-takeover");
            socket.write_all(&first[..3]).await.unwrap();
            let bytes = [
                first[3..].to_vec(),
                subscribe(version, &[0, 1, 2]),
                publish(version, 0, 0, false),
                publish(version, 1, 33, false),
                publish(version, 2, 34, false),
                vec![0x62, 2, 0, 34, 0xc0, 0, 0xe0, 0],
            ]
            .concat();
            socket.write_all(&bytes).await.unwrap();
            assert_eq!(
                read_frame(&mut socket).await,
                if version == 5 {
                    vec![0x20, 3, 1, 0, 0]
                } else {
                    vec![0x20, 2, 1, 0]
                }
            );
            assert_eq!(
                read_frame(&mut socket).await,
                if version == 5 {
                    vec![0x90, 6, 0, 7, 0, 0, 1, 2]
                } else {
                    vec![0x90, 5, 0, 7, 0, 1, 2]
                }
            );
            for expected in [
                vec![0x40, 2, 0, 33],
                vec![0x50, 2, 0, 34],
                vec![0x70, 2, 0, 34],
                vec![0xd0, 0],
            ] {
                assert_eq!(read_frame(&mut socket).await, expected);
            }
            assert_eq!(
                timeout(Duration::from_secs(2), socket.read_u8())
                    .await
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        });
    }
    while let Some(result) = clients.join_next().await {
        result.unwrap();
    }
    wait_until(|| server.metrics.snapshot().connections == 0).await;
    let s = server.metrics.snapshot();
    assert_eq!((s.conn_rcvd, s.pub_rcvd, s.sub_rcvd), (24, 72, 24));
    assert_eq!(
        (s.sessions, s.subs, s.pub_qos0_sent, s.pub_qos1_sent),
        (0, 0, 0, 0)
    );
    server.shutdown().await;
}

struct Handler;
#[async_trait::async_trait]
impl flowsdk::mqtt_client::TokioMqttEventHandler for Handler {}

#[tokio::test]
async fn flowsdk_clients_complete_both_versions_and_qos_handshakes() {
    use flowsdk::mqtt_client::{MqttClientOptions, TokioAsyncClientConfig, TokioAsyncMqttClient};
    let server = TestServer::start(ServerConfig::default()).await;
    timeout(Duration::from_secs(10), async {
        for version in [4, 5] {
            let mut options = MqttClientOptions::builder()
                .peer(server.mqtt_addr.to_string())
                .client_id("flowsdk-perf-test")
                .reconnect(false)
                .keep_alive(0)
                .build();
            options.mqtt_version = version;
            let config = TokioAsyncClientConfig::builder()
                .auto_reconnect(false)
                .build();
            let client = TokioAsyncMqttClient::new(options, Box::new(Handler), config)
                .await
                .unwrap();
            assert!(client.connect_sync().await.unwrap().is_success());
            assert!(client
                .subscribe_sync("perf/+", 2)
                .await
                .unwrap()
                .is_success());
            for qos in [0, 1, 2] {
                assert!(client
                    .publish_sync("perf/topic", b"test", qos, false)
                    .await
                    .unwrap()
                    .is_success());
            }
            assert!(client
                .unsubscribe_sync(vec!["perf/+"])
                .await
                .unwrap()
                .is_success());
            assert!(client.ping_sync().await.unwrap().success);
            client.disconnect_sync().await.unwrap();
            client.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
    wait_until(|| server.metrics.snapshot().connections == 0).await;
    assert_eq!(server.metrics.snapshot().pub_rcvd, 6);
    server.shutdown().await;
}

#[tokio::test]
async fn eof_rejected_frames_and_shutdown_release_connections() {
    let server = TestServer::start(ServerConfig::default()).await;
    let pending = TcpStream::connect(server.mqtt_addr).await.unwrap();
    wait_until(|| server.metrics.snapshot().connections == 1).await;
    assert_eq!(server.metrics.snapshot().conn_rcvd, 0);
    drop(pending);
    wait_until(|| server.metrics.snapshot().connections == 0).await;
    let mut bad = server.client(5).await;
    bad.write_all(&[0x32, 1, 0]).await.unwrap();
    wait_until(|| server.metrics.snapshot().connections == 0).await;
    assert_eq!(server.metrics.snapshot().pub_rcvd, 0);
    let active = server.client(4).await;
    let pending = TcpStream::connect(server.mqtt_addr).await.unwrap();
    wait_until(|| server.metrics.snapshot().connections == 2).await;
    server.shutdown().await;
    drop((active, pending));
}

#[tokio::test]
async fn cancelling_the_server_releases_connection_guards() {
    let server = TestServer::start(ServerConfig::default()).await;
    let mut socket = server.client(5).await;
    socket.write_all(&publish(5, 1, 8, false)).await.unwrap();
    assert_eq!(read_frame(&mut socket).await, vec![0x40, 2, 0, 8]);
    server.task.abort();
    wait_until(|| server.metrics.snapshot().connections == 0).await;
    assert_eq!(server.metrics.snapshot().pub_rcvd, 1);
}

#[tokio::test]
async fn metrics_can_be_disabled_and_bind_conflicts_fail_startup() {
    let server = TestServer::start(ServerConfig {
        metrics_listen: None,
        ..Default::default()
    })
    .await;
    assert!(server.metrics_addr.is_none());
    let socket = server.client(5).await;
    drop(socket);
    server.shutdown().await;
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        metrics_listen: Some(occupied.local_addr().unwrap()),
        ..Default::default()
    };
    assert_eq!(
        TcpServer::bind(config).await.err().unwrap().kind(),
        std::io::ErrorKind::AddrInUse
    );
}
