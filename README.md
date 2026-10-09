# mqtt_tcp_server

`PerfServerEngine` is a TCP MQTT 3.1.1/5 acknowledgment endpoint for testing client
performance. It accepts CONNECT and SUBSCRIBE, discards incoming PUBLISH data, and
completes QoS 1/2 handshakes. It has no session store, routing, subscriber delivery,
retained messages, Will delivery, authentication, TLS, or QUIC.

The runtime enables `std` explicitly with default features disabled; tests also
enable `async-client`.

```sh
cd mqtt_tcp_server
cargo run --release -- \
  --listen 127.0.0.1:1883 --metrics-listen 127.0.0.1:9464

# In another terminal:
curl --fail http://127.0.0.1:9464/metrics
```

## MQTT behavior

| Input         | Action                                                                                                                         |
|---------------|--------------------------------------------------------------------------------------------------------------------------------|
| CONNECT       | Success CONNACK. By default Session Present is true whenever Clean Start/Clean Session is false, even for an unknown ClientID. |
| SUBSCRIBE     | SUBACK grants each requested QoS 0/1/2. No filters are stored.                                              |
| PUBLISH QoS 0 | Count and discard; no acknowledgment.                                                                                          |
| PUBLISH QoS 1 | PUBACK success.                                                                                                                |
| PUBLISH QoS 2 | PUBREC success.                                                                                                                |
| PUBREL        | PUBCOMP success, even without a preceding PUBLISH.                                                                             |
| UNSUBSCRIBE   | UNSUBACK success (one result per filter for MQTT 5).                                                                           |
| PINGREQ       | PINGRESP.                                                                                                                      |
| DISCONNECT    | Drain already queued replies and close.                                                                                        |

Duplicates get the same response and count as additional received packets. There
is no deduplication or state shared between connections, and duplicate ClientIDs
do not cause takeover. `--fake-session-recovery disabled` always clears Session
Present. Empty MQTT 5 ClientIDs receive a generated Assigned Client Identifier.

The engine uses FlowSDK's layered fixed-header parser with a borrowed body view
and bounded byte cursors. It checks framing, lengths needed to build acknowledgments,
protocol level, and buffer limits. It deliberately skips topic syntax, UTF-8, property
contents, credentials, reserved flag combinations, and QoS/session sequencing.
CONNECT must come first; a second CONNECT or unsupported direction closes the
connection. Reserved QoS bits in PUBLISH/SUBSCRIBE, empty subscription lists,
truncated required fields, and oversized frames/responses also close it. Keepalive
is not enforced.

Defaults: 1 MiB maximum frame/input, 64 KiB queued output, and 256 packets per
engine call. A frame limit includes its fixed header. The input limit must cover
a maximum frame. The engine buffers at most one frame at a time, reuses input and
output storage, and admits the body only after checking its declared size. A
response must fit the configured output limit; temporary output backpressure
pauses processing without losing or duplicating responses.

## Prometheus metrics

`GET /metrics` exposes these exact names in Prometheus text format 0.0.4, without
labels. Each series is present even when zero. Scrapes are read-only snapshots;
counter values survive disconnects and reset on process restart.

| Name            | Type    | Meaning                                                                           |
|-----------------|---------|-----------------------------------------------------------------------------------|
| `sessions`      | Gauge   | Stored sessions; always zero. Fake recovery does not create sessions.             |
| `connections`   | Gauge   | Current MQTT TCP connections, including pending CONNECT and draining sockets.     |
| `subs`          | Gauge   | Stored subscriptions; always zero.                                                |
| `conn_rcvd`     | Counter | CONNECT packets accepted by the minimal parser/phase checks.                      |
| `pub_rcvd`      | Counter | Accepted PUBLISH packets across QoS 0/1/2, including duplicates. Excludes PUBREL. |
| `sub_rcvd`      | Counter | Accepted SUBSCRIBE packets, not filter entries. Excludes UNSUBSCRIBE.             |
| `pub_qos0_sent` | Counter | Outbound QoS 0 PUBLISH packets; always zero.                                      |
| `pub_qos1_sent` | Counter | Outbound QoS 1 PUBLISH packets; always zero.                                      |

ACKs do not count as outbound PUBLISH. Received counters advance when a complete
packet is consumed and its response is queued, if needed. Partial/rejected frames
and retries caused by output backpressure do not count. Receipt counts remain
even if a subsequent write fails. Engine counters are aggregated once per batch;
simultaneous metric updates need not appear atomically in one scrape.

HTTP uses Hyper, accepts at most 16 concurrent scrape connections, limits header
buffering to 8 KiB, and closes each connection after its response. A five-second
deadline bounds each entire HTTP exchange. 

Example Prometheus configuration:

```yaml
scrape_configs:
  - job_name: mqtt_tcp_server
    scrape_interval: 5s
    static_configs:
      - targets: ['127.0.0.1:9464']
```

Use `rate(pub_rcvd[1m])` for received publications per second and
`rate(conn_rcvd[1m])` for MQTT connection establishments per second.

## Library and verification

The crate exports `PerfServerEngine`, `PerfServerConfig`, `TcpServer`, and
`BrokerMetrics`. `TcpServer::bind` supports port zero and exposes actual listener
addresses. `serve(shutdown_future)` provides bounded shutdown for embedded use.

For a custom transport driver, retain `feed()`'s unconsumed input suffix. Write
`output()` and advance only bytes actually written with `consume_output()`.
Call `poll()` when `buffered_work` is set, after draining output as needed. EOF
drops the engine. The private `src/wire.rs` adapter uses FlowSDK's `parse_type_only`
on complete frames, then borrows the body from the engine buffer. It keeps the
same RawBody-depth checks without payload copies. 

```sh
# From this repository:
cargo test --locked
cargo test --locked --features flowsdk/strict-protocol-compliance
cargo clippy --locked --all-targets -- -D warnings
```

The tests exercise both versions with FlowSDK clients, QoS 0/1/2, fake recovery,
fragmentation, partial writes/backpressure, permissive fields, concurrent TCP
connections, scrape accounting, errors, cancellation, and shutdown.

For sustained Linux benchmarking, use the existing
[`mqtt_ring_bench`](https://github.com/emqx/flowsdk/tree/main/mqtt_ring_bench) connect/publish workloads against
this server. Subscriber workloads require actual deliveries, which this endpoint
does not produce. Record build mode, client/connection counts, packet sizes, QoS,
scrape interval, and metrics-enabled/disabled status with each result. Client
enqueue rate is different from `pub_rcvd` reception rate, especially at QoS 0.

## Tagged builds

Every tag builds static Linux binaries for:

- `x86_64-unknown-linux-musl`
- `aarch64-unknown-linux-musl`

Pushing a tag starting with `v` (for example, `v0.1.0`) also creates a
[GitHub Release](https://github.com/qzhuyan/MQTTBenchServer/releases) after tests
and both builds pass. The release includes both `.tar.gz` archives, their SHA-256
checksums, and generated release notes. Rerunning the job updates the assets on
an existing release. Other tags provide build artifacts in the CI run.
