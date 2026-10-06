// SPDX-License-Identifier: MPL-2.0

//! Sessionless MQTT 3.1.1/5 acknowledgment server for client performance testing.
//! Success responses do not imply storage, routing, or actual session recovery.

pub mod config;
pub mod metrics;
pub mod perf;
pub mod stats;
pub mod tcp;

pub use config::{FakeSessionRecovery, PerfServerConfig, ServerConfig};
pub use perf::{PerfProgress, PerfServerEngine, PerfServerError};
pub use stats::{BrokerMetrics, MetricsSnapshot, PerfServerStats};
pub use tcp::TcpServer;

// Private adapters for the published FlowSDK parser and MQTT reply encoding.
mod wire;
