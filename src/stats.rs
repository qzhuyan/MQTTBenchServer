// SPDX-License-Identifier: MPL-2.0

use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct PerfServerStats {
    pub packets_rcvd: u64,
    pub conn_rcvd: u64,
    pub pub_rcvd: u64,
    pub sub_rcvd: u64,
    pub pub_rcvd_by_qos: [u64; 3],
    pub pubrel_rcvd: u64,
    pub unsub_rcvd: u64,
    pub subscribe_entries: u64,
    pub response_bytes_written: u64,
}

/// Process totals. No per-client labels, registry locks, or packet-path formatting.
#[derive(Debug, Default)]
pub struct BrokerMetrics {
    connections: AtomicU64,
    conn_rcvd: AtomicU64,
    pub_rcvd: AtomicU64,
    sub_rcvd: AtomicU64,
    packets_rcvd: AtomicU64,
    pub_rcvd_by_qos: [AtomicU64; 3],
    pubrel_rcvd: AtomicU64,
    subscribe_entries: AtomicU64,
    response_bytes_written: AtomicU64,
    connection_errors: AtomicU64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub sessions: u64,
    pub connections: u64,
    pub subs: u64,
    pub conn_rcvd: u64,
    pub pub_rcvd: u64,
    pub sub_rcvd: u64,
    pub pub_qos0_sent: u64,
    pub pub_qos1_sent: u64,
}

impl BrokerMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections: self.connections.load(Relaxed),
            conn_rcvd: self.conn_rcvd.load(Relaxed),
            pub_rcvd: self.pub_rcvd.load(Relaxed),
            sub_rcvd: self.sub_rcvd.load(Relaxed),
            ..MetricsSnapshot::default()
        }
    }

    pub fn render(&self) -> String {
        let s = self.snapshot();
        let mut out = String::with_capacity(1400);
        for (name, kind, help, value) in [
            (
                "sessions",
                "gauge",
                "Stored MQTT sessions (always zero in this sessionless server).",
                s.sessions,
            ),
            (
                "connections",
                "gauge",
                "Current MQTT TCP connections including pending CONNECT and draining sockets.",
                s.connections,
            ),
            (
                "subs",
                "gauge",
                "Stored subscriptions (always zero in this server).",
                s.subs,
            ),
            (
                "conn_rcvd",
                "counter",
                "CONNECT packets accepted for minimal handling since startup.",
                s.conn_rcvd,
            ),
            (
                "pub_rcvd",
                "counter",
                "PUBLISH packets accepted at all QoS levels including duplicates since startup.",
                s.pub_rcvd,
            ),
            (
                "sub_rcvd",
                "counter",
                "SUBSCRIBE packets accepted since startup (not filter entries).",
                s.sub_rcvd,
            ),
            (
                "pub_qos0_sent",
                "counter",
                "Complete outbound QoS 0 PUBLISH writes (always zero in this server).",
                s.pub_qos0_sent,
            ),
            (
                "pub_qos1_sent",
                "counter",
                "Complete outbound QoS 1 PUBLISH writes (always zero in this server).",
                s.pub_qos1_sent,
            ),
        ] {
            writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}"
            )
            .unwrap();
        }
        out
    }

    /// Detailed benchmark totals, separate from the eight exported series.
    pub fn summary(&self) -> String {
        format!("{:?} packets={} publish_qos={:?} pubrel={} subscribe_entries={} response_bytes={} connection_errors={}",
            self.snapshot(), self.packets_rcvd.load(Relaxed),
            self.pub_rcvd_by_qos.each_ref().map(|v| v.load(Relaxed)),
            self.pubrel_rcvd.load(Relaxed), self.subscribe_entries.load(Relaxed),
            self.response_bytes_written.load(Relaxed), self.connection_errors.load(Relaxed))
    }

    pub(crate) fn opened(&self) {
        self.connections.fetch_add(1, Relaxed);
    }
    pub(crate) fn closed(&self) {
        self.connections.fetch_sub(1, Relaxed);
    }
    pub(crate) fn error(&self) {
        self.connection_errors.fetch_add(1, Relaxed);
    }
    pub(crate) fn publish_delta(&self, now: &PerfServerStats, old: &mut PerfServerStats) {
        for (metric, current, previous) in [
            (&self.conn_rcvd, now.conn_rcvd, old.conn_rcvd),
            (&self.pub_rcvd, now.pub_rcvd, old.pub_rcvd),
            (&self.sub_rcvd, now.sub_rcvd, old.sub_rcvd),
            (&self.packets_rcvd, now.packets_rcvd, old.packets_rcvd),
            (&self.pubrel_rcvd, now.pubrel_rcvd, old.pubrel_rcvd),
            (
                &self.subscribe_entries,
                now.subscribe_entries,
                old.subscribe_entries,
            ),
            (
                &self.response_bytes_written,
                now.response_bytes_written,
                old.response_bytes_written,
            ),
        ] {
            if current != previous {
                metric.fetch_add(current - previous, Relaxed);
            }
        }
        for i in 0..3 {
            let delta = now.pub_rcvd_by_qos[i] - old.pub_rcvd_by_qos[i];
            if delta != 0 {
                self.pub_rcvd_by_qos[i].fetch_add(delta, Relaxed);
            }
        }
        *old = *now;
    }
}
