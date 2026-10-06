// SPDX-License-Identifier: MPL-2.0
#![allow(dead_code)]

use mqtt_tcp_server::{BrokerMetrics, ServerConfig, TcpServer};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

pub fn frame(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    let mut len = body.len();
    loop {
        let byte = (len % 128) as u8;
        len /= 128;
        out.push(byte | if len > 0 { 128 } else { 0 });
        if len == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

pub fn connect(version: u8, clean: bool, id: &[u8]) -> Vec<u8> {
    let mut body = vec![
        0,
        4,
        b'M',
        b'Q',
        b'T',
        b'T',
        version,
        if clean { 2 } else { 0 },
        0,
        0,
    ];
    if version == 5 {
        body.push(0);
    }
    body.extend_from_slice(&(id.len() as u16).to_be_bytes());
    body.extend_from_slice(id);
    frame(0x10, &body)
}

pub fn publish(version: u8, qos: u8, id: u16, dup: bool) -> Vec<u8> {
    let mut body = vec![0, 1, b't'];
    if qos != 0 {
        body.extend_from_slice(&id.to_be_bytes());
    }
    if version == 5 {
        body.push(0);
    }
    body.extend_from_slice(b"payload");
    frame(0x30 | qos << 1 | if dup { 8 } else { 0 }, &body)
}

pub fn subscribe(version: u8, options: &[u8]) -> Vec<u8> {
    let mut body = vec![0, 7];
    if version == 5 {
        body.push(0);
    }
    for &option in options {
        body.extend_from_slice(&[0, 1, b't', option]);
    }
    frame(0x82, &body)
}

pub async fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    timeout(Duration::from_secs(3), async {
        let first = stream.read_u8().await.unwrap();
        let mut bytes = vec![first];
        let mut len = 0;
        for shift in [0, 7, 14, 21] {
            let b = stream.read_u8().await.unwrap();
            bytes.push(b);
            len |= ((b & 127) as usize) << shift;
            if b & 128 == 0 {
                break;
            }
        }
        let offset = bytes.len();
        bytes.resize(offset + len, 0);
        stream.read_exact(&mut bytes[offset..]).await.unwrap();
        bytes
    })
    .await
    .expect("MQTT response timeout")
}

pub async fn wait_until(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition did not become true");
}

pub struct TestServer {
    pub mqtt_addr: SocketAddr,
    pub metrics_addr: Option<SocketAddr>,
    pub metrics: Arc<BrokerMetrics>,
    stop: Option<oneshot::Sender<()>>,
    pub task: JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    pub async fn start(mut config: ServerConfig) -> Self {
        config.listen = "127.0.0.1:0".parse().unwrap();
        if config.metrics_listen.is_some() {
            config.metrics_listen = Some("127.0.0.1:0".parse().unwrap());
        }
        let server = TcpServer::bind(config).await.unwrap();
        let mqtt_addr = server.local_addr().unwrap();
        let metrics_addr = server.metrics_addr().unwrap();
        let metrics = server.metrics();
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(server.serve(async {
            let _ = rx.await;
        }));
        Self {
            mqtt_addr,
            metrics_addr,
            metrics,
            stop: Some(tx),
            task,
        }
    }
    pub async fn shutdown(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        timeout(Duration::from_secs(3), &mut self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(self.metrics.snapshot().connections, 0);
    }
    pub async fn client(&self, version: u8) -> TcpStream {
        let mut socket = TcpStream::connect(self.mqtt_addr).await.unwrap();
        socket
            .write_all(&connect(version, false, b"test"))
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut socket).await,
            if version == 5 {
                vec![0x20, 3, 1, 0, 0]
            } else {
                vec![0x20, 2, 1, 0]
            }
        );
        socket
    }
    pub async fn scrape(&self, request: &str) -> String {
        let mut stream = TcpStream::connect(self.metrics_addr.unwrap())
            .await
            .unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        timeout(Duration::from_secs(3), stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        response
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
