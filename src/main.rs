// SPDX-License-Identifier: MPL-2.0

use clap::Parser;
use mqtt_tcp_server::{FakeSessionRecovery, PerfServerConfig, ServerConfig, TcpServer};
use std::{net::SocketAddr, time::Duration};

#[derive(Parser)]
#[command(
    about = "Sessionless MQTT 3.1.1/5 TCP acknowledgment server for client performance tests"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:1883")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9464")]
    metrics_listen: SocketAddr,
    #[arg(long)]
    no_metrics: bool,
    #[arg(long, value_enum, default_value = "when-requested")]
    fake_session_recovery: FakeSessionRecovery,
    #[arg(long, default_value_t = 1_048_576)]
    max_frame_bytes: usize,
    #[arg(long, default_value_t = 1_048_576)]
    max_input_bytes: usize,
    #[arg(long, default_value_t = 65_536)]
    max_output_bytes: usize,
    #[arg(long, default_value_t = 256)]
    max_packets_per_poll: usize,
    #[arg(long, default_value_t = 10)]
    write_timeout_secs: u64,
    #[arg(long, default_value_t = 5)]
    shutdown_timeout_secs: u64,
    #[arg(long, default_value_t = 5)]
    metrics_timeout_secs: u64,
    #[arg(long, default_value_t = 16)]
    max_scrapes: usize,
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = ServerConfig {
        listen: args.listen,
        metrics_listen: (!args.no_metrics).then_some(args.metrics_listen),
        engine: PerfServerConfig {
            fake_session_recovery: args.fake_session_recovery,
            max_frame_bytes: args.max_frame_bytes,
            max_input_bytes: args.max_input_bytes,
            max_output_bytes: args.max_output_bytes,
            max_packets_per_poll: args.max_packets_per_poll,
            ..PerfServerConfig::default()
        },
        write_timeout: Duration::from_secs(args.write_timeout_secs),
        shutdown_timeout: Duration::from_secs(args.shutdown_timeout_secs),
        metrics_timeout: Duration::from_secs(args.metrics_timeout_secs),
        max_scrapes: args.max_scrapes,
    };
    let server = TcpServer::bind(config).await?;
    eprintln!("PerfServerEngine MQTT TCP: {}", server.local_addr()?);
    if let Some(addr) = server.metrics_addr()? {
        eprintln!("Prometheus: http://{addr}/metrics");
    }
    let metrics = server.metrics();
    let result = server.serve(shutdown_signal()).await;
    eprintln!("{}", metrics.summary());
    result?;
    Ok(())
}
