// SPDX-License-Identifier: MPL-2.0

use crate::wire::MAX_REMAINING_LENGTH;
use clap::ValueEnum;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum FakeSessionRecovery {
    #[default]
    WhenRequested,
    Disabled,
}

#[derive(Debug, Clone)]
pub struct PerfServerConfig {
    pub mqtt311: bool,
    pub mqtt5: bool,
    pub fake_session_recovery: FakeSessionRecovery,
    /// Maximum whole frame size, including the fixed header.
    pub max_frame_bytes: usize,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub max_packets_per_poll: usize,
}

impl Default for PerfServerConfig {
    fn default() -> Self {
        Self {
            mqtt311: true,
            mqtt5: true,
            fake_session_recovery: FakeSessionRecovery::WhenRequested,
            max_frame_bytes: 8 * 1024,
            max_input_bytes: 8 * 1024,
            max_output_bytes: 64 * 1024,
            max_packets_per_poll: 256,
        }
    }
}

impl PerfServerConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !self.mqtt311 && !self.mqtt5 {
            return Err("at least one MQTT version must be enabled");
        }
        if !(2..=MAX_REMAINING_LENGTH + 5).contains(&self.max_frame_bytes) {
            return Err("max frame bytes must be between 2 and 268435460");
        }
        if self.max_input_bytes < self.max_frame_bytes {
            return Err("max input bytes must cover one maximum frame");
        }
        if self.max_output_bytes < 5 || self.max_output_bytes > MAX_REMAINING_LENGTH + 5 {
            return Err("max output bytes must be between 5 and 268435460");
        }
        if self.max_packets_per_poll == 0 {
            return Err("max packets per poll must be positive");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub metrics_listen: Option<SocketAddr>,
    pub engine: PerfServerConfig,
    /// Per-connection TCP read scratch space; independent of the MQTT frame limit.
    pub read_buffer_bytes: usize,
    pub write_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub metrics_timeout: Duration,
    pub max_scrapes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let local = IpAddr::V4(Ipv4Addr::LOCALHOST);
        Self {
            listen: SocketAddr::new(local, 1883),
            metrics_listen: Some(SocketAddr::new(local, 9464)),
            engine: PerfServerConfig::default(),
            read_buffer_bytes: 1024,
            write_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
            metrics_timeout: Duration::from_secs(5),
            max_scrapes: 16,
        }
    }
}

impl ServerConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.engine.validate()?;
        if !(1..=1024 * 1024).contains(&self.read_buffer_bytes) {
            return Err("read buffer bytes must be between 1 and 1048576");
        }
        if self.write_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self.metrics_timeout.is_zero()
        {
            return Err("timeouts must be positive");
        }
        if self.max_scrapes == 0 {
            return Err("max scrapes must be positive");
        }
        Ok(())
    }
}
