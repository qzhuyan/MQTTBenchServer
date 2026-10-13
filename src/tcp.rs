// SPDX-License-Identifier: MPL-2.0

use crate::{
    metrics::serve_scrape, BrokerMetrics, PerfServerEngine, PerfServerStats, ServerConfig,
};
use std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
    time::timeout,
};

/// Both listeners are bound before any connections are admitted.
pub struct TcpServer {
    config: ServerConfig,
    listener: TcpListener,
    metrics_listener: Option<TcpListener>,
    metrics: Arc<BrokerMetrics>,
}

impl TcpServer {
    pub async fn bind(config: ServerConfig) -> io::Result<Self> {
        config
            .validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let listener = TcpListener::bind(config.listen).await?;
        let metrics_listener = match config.metrics_listen {
            Some(addr) => Some(TcpListener::bind(addr).await?),
            None => None,
        };
        Ok(Self {
            config,
            listener,
            metrics_listener,
            metrics: Arc::new(BrokerMetrics::default()),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
    pub fn metrics_addr(&self) -> io::Result<Option<SocketAddr>> {
        self.metrics_listener
            .as_ref()
            .map(TcpListener::local_addr)
            .transpose()
    }
    pub fn metrics(&self) -> Arc<BrokerMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Runs until shutdown resolves or a listener fails. Connection failures are
    /// isolated. In-flight writes get a bounded drain, then tasks are cancelled.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) -> io::Result<()> {
        let Self {
            config,
            listener,
            metrics_listener,
            metrics,
        } = self;
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut clients = JoinSet::new();
        let mut scrapes = JoinSet::new();
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                _ = &mut shutdown => break Ok(()),
                Some(result) = clients.join_next(), if !clients.is_empty() => {
                    if result.is_err() { metrics.error(); }
                },
                Some(_) = scrapes.join_next(), if !scrapes.is_empty() => {},
                accepted = listener.accept() => {
                    let (socket, _) = match accepted { Ok(v) => v, Err(e) => break Err(e) };
                    let engine = PerfServerEngine::new(config.engine.clone())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                    // Construct before spawning so cancellation before the first
                    // poll still releases the counted connection.
                    let state = ConnectionState::new(engine, Arc::clone(&metrics));
                    let stop = stop_rx.clone();
                    let write_timeout = config.write_timeout;
                    let read_buffer_bytes = config.read_buffer_bytes.min(config.engine.max_frame_bytes);
                    clients.spawn(async move {
                        let mut state = state;
                        if drive(socket, &mut state, write_timeout, read_buffer_bytes, stop).await.is_err() {
                            state.metrics.error();
                        }
                    });
                },
                accepted = accept_metrics(&metrics_listener), if metrics_listener.is_some() && scrapes.len() < config.max_scrapes => {
                    let (socket, _) = match accepted { Ok(v) => v, Err(e) => break Err(e) };
                    scrapes.spawn(serve_scrape(socket, Arc::clone(&metrics), config.metrics_timeout, stop_rx.clone()));
                },
            }
        };
        drop(listener);
        drop(metrics_listener);
        let _ = stop_tx.send(true);
        if timeout(config.shutdown_timeout, async {
            while clients.join_next().await.is_some() {}
            while scrapes.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            clients.abort_all();
            scrapes.abort_all();
            while clients.join_next().await.is_some() {}
            while scrapes.join_next().await.is_some() {}
        }
        result
    }
}

async fn accept_metrics(listener: &Option<TcpListener>) -> io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}

struct ConnectionState {
    engine: PerfServerEngine,
    metrics: Arc<BrokerMetrics>,
    published: PerfServerStats,
}

impl ConnectionState {
    fn new(engine: PerfServerEngine, metrics: Arc<BrokerMetrics>) -> Self {
        metrics.opened();
        Self {
            engine,
            metrics,
            published: PerfServerStats::default(),
        }
    }
    fn flush_stats(&mut self) {
        self.metrics
            .publish_delta(self.engine.stats(), &mut self.published);
    }
}

impl Drop for ConnectionState {
    fn drop(&mut self) {
        self.flush_stats();
        self.metrics.closed();
    }
}

async fn drain<W: AsyncWrite + Unpin>(
    socket: &mut W,
    state: &mut ConnectionState,
    deadline: Duration,
) -> io::Result<()> {
    timeout(deadline, async {
        while !state.engine.output().is_empty() {
            let written = socket.write(state.engine.output()).await?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            state
                .engine
                .consume_output(written)
                .map_err(io::Error::other)?;
        }
        Ok(())
    })
    .await
    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

async fn drive(
    mut socket: TcpStream,
    state: &mut ConnectionState,
    write_timeout: Duration,
    read_buffer_bytes: usize,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    socket.set_nodelay(true)?;
    // Keep the async task small even when a caller opts into larger read batches.
    let mut bytes = vec![0u8; read_buffer_bytes].into_boxed_slice();
    let (mut offset, mut end) = (0, 0);
    loop {
        if *stop.borrow() {
            drain(&mut socket, state, write_timeout).await?;
            return Ok(());
        }
        let progress = state.engine.feed(&bytes[offset..end]);
        state.flush_stats(); // Also publish successfully processed frames before an error.
        let progress = progress.map_err(io::Error::other)?;
        offset += progress.consumed_input;
        if progress.needs_output {
            drain(&mut socket, state, write_timeout).await?;
            state.flush_stats();
        }
        if state.engine.is_closed() {
            return Ok(());
        }
        if offset < end || progress.buffered_work {
            tokio::task::yield_now().await;
            continue;
        }
        let allowance = state.engine.read_allowance().min(bytes.len());
        if allowance == 0 {
            return Err(io::Error::other("engine made no progress"));
        }
        tokio::select! {
            _ = stop.changed() => {},
            read = socket.read(&mut bytes[..allowance]) => {
                end = read?;
                offset = 0;
                if end == 0 { return Ok(()); }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_replies() -> ConnectionState {
        let mut engine = PerfServerEngine::new(Default::default()).unwrap();
        engine
            .feed(&[
                0x10, 14, 0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 0, 0, 0, 1, b'c', 0x32, 6, 0, 1,
                b't', 0, 1, 0,
            ])
            .unwrap();
        ConnectionState::new(engine, Arc::new(BrokerMetrics::default()))
    }

    #[tokio::test]
    async fn connection_future_does_not_embed_the_read_buffer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let mut state = pending_replies();
        let (_tx, rx) = watch::channel(false);
        let driver = drive(socket, &mut state, Duration::from_secs(1), 64 * 1024, rx);
        // A larger configured read batch must not inflate the async task itself.
        let size = std::mem::size_of_val(&driver);
        assert!(size < 2048, "connection future is {size} bytes");
    }

    #[tokio::test]
    async fn failed_write_preserves_receipt_counts_on_teardown() {
        let mut state = pending_replies();
        let metrics = Arc::clone(&state.metrics);
        let (mut writer, reader) = tokio::io::duplex(1);
        drop(reader);
        assert!(drain(&mut writer, &mut state, Duration::from_secs(1))
            .await
            .is_err());
        drop(state); // Flush even when no normal batch publication occurred.
        let snapshot = metrics.snapshot();
        assert_eq!(
            (snapshot.connections, snapshot.conn_rcvd, snapshot.pub_rcvd),
            (0, 1, 1)
        );
    }

    #[tokio::test]
    async fn partial_write_deadline_releases_connection_without_losing_totals() {
        let mut state = pending_replies();
        let metrics = Arc::clone(&state.metrics);
        let (mut writer, _stalled_reader) = tokio::io::duplex(1);
        let error = drain(&mut writer, &mut state, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(state.engine.stats().response_bytes_written, 1);
        assert_eq!(state.engine.output().len(), 8);
        drop(state);
        assert_eq!(metrics.snapshot().connections, 0);
        assert_eq!(metrics.snapshot().pub_rcvd, 1);
    }
}
