# Configuration

Every option is a flag, and can also be set with the environment variable shown. `rmf-pipes --help` lists them all, and never prints their current values.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--listen` | `RMF_PIPES_LISTEN` | `127.0.0.1:18080` | HTTP address. Port `0` picks a free port, and the log line `listening` reports it. |
| `--log-format` | `RMF_PIPES_LOG_FORMAT` | `pretty` | `pretty` or `json` (one object per line). The filter comes from `RUST_LOG` (default `info`). |
| `--shutdown-drain` | `RMF_PIPES_SHUTDOWN_DRAIN` | `5s` | How long open connections get to finish after SIGINT/SIGTERM. |
| `--zenoh-connect` | `RMF_PIPES_ZENOH_CONNECT` | `tcp/127.0.0.1:7447` | zenoh endpoints, comma-separated. The default is a local `rmw_zenohd`. |
| `--zenoh-mode` | `RMF_PIPES_ZENOH_MODE` | `client` | `client` (through a router, like rmw_zenoh nodes) or `peer`. |
| `--zenoh-config` | `RMF_PIPES_ZENOH_CONFIG` | — | A zenoh JSON5 config file, applied before the options above (e.g. for TLS or ACL credentials). |
| `--domain-id` | `ROS_DOMAIN_ID` | `0` | ROS domain. |
| `--node-name` | `RMF_PIPES_NODE_NAME` | `rmf_pipes` | The ROS node name rmf-pipes appears as. Its own entities are hidden from the graph it reports. Give each instance a distinct name: another node with the same name is hidden from the node list (its endpoints still show). |
| `--ingest` | `RMF_PIPES_INGEST` | — | Standing topic patterns, comma-separated. `*` matches one segment and `**` any number, e.g. `fleet/*/state,door_states`. Other topics are subscribed only while someone watches them. |
| `--idle-timeout` | `RMF_PIPES_IDLE_TIMEOUT` | `30s` | How long an on-demand topic stays subscribed after its last watcher leaves. |
| `--max-rate-hz` | `RMF_PIPES_MAX_RATE_HZ` | `20` | Per-topic admission cap, `0` (off) or 0.001–1e6. Within each interval only the **newest** sample is delivered, at the end of the interval; the ones it replaced are counted as `rate_limited`. |
| `--queue-depth` | `RMF_PIPES_QUEUE_DEPTH` | `16` | Per-subscription queue between zenoh and the hub (1–10000). The oldest samples are dropped when the hub falls behind; hiroz doesn't count these drops. For transient_local publishers the depth is raised to their history depth (up to 100), so late-join history isn't truncated. |
| `--max-payload-bytes` | `RMF_PIPES_MAX_PAYLOAD_BYTES` | `1048576` | Larger payloads are delivered with `data: null, omitted: "too_large"`, and are not decoded. |
| `--max-topics` | `RMF_PIPES_MAX_TOPICS` | `256` | Most topics subscribed **on demand** at once (standing topics don't count). Further new topics get `503 too_many_topics`. Recording streams don't count (see `--max-record-streams`). |
| `--max-streams` | `RMF_PIPES_MAX_STREAMS` | `256` | Most concurrent SSE streams. Further streams get `503 too_many_streams`. |
| `--database-url` | `RMF_PIPES_DATABASE_URL` | — | Postgres URL. Holds the audit log and recordings, and **enables authentication** unless `--demo`. Without it the API is anonymous and read-only, and only a loopback `--listen` is allowed. Never echoed, even in errors. It usually contains a password: prefer the env var, so it stays out of `ps`. |
| `--demo` | `RMF_PIPES_DEMO` | off | **Demo mode**: no authentication, and every caller acts as an admin, so allowlisted writes and recording-rule changes work without logging in. With a database, the audit log and recordings still go there; without one, audit entries go to the process log. Refused on a non-loopback `--listen`, even with `--insecure-no-auth`. Two checks stop a web page from driving it from the browser: every non-GET request must be `application/json`, and every request must be addressed to `localhost` or a loopback IP (`Host`), with any browser `Origin` naming that same host (against DNS rebinding). Open the dashboard as `http://localhost:…` or `http://127.0.0.1:…`, not through another hostname. |
| `--database-connect-timeout` | `RMF_PIPES_DATABASE_CONNECT_TIMEOUT` | `10s` | How long to wait for Postgres at startup. |
| `--bootstrap-admin` | `RMF_PIPES_BOOTSTRAP_ADMIN` | — | If the database has no users, create this admin at startup… |
| `--bootstrap-password` | `RMF_PIPES_BOOTSTRAP_PASSWORD` | — | …with this password. Prefer the env var, so it stays out of `ps`. |
| `--session-idle` | `RMF_PIPES_SESSION_IDLE` | `12h` | A browser session ends after this long without use (1m–30d). |
| `--session-max` | `RMF_PIPES_SESSION_MAX` | `7d` | A browser session ends this long after login, however much it is used (1m–30d; at least `--session-idle`). |
| `--cookie-secure` | `RMF_PIPES_COOKIE_SECURE` | off | Mark the session cookie `Secure`, and name it `__Host-rmf_pipes_session` so sibling subdomains can't plant or shadow it. Use it whenever clients connect over HTTPS. |
| `--insecure-no-auth` | `RMF_PIPES_INSECURE_NO_AUTH` | off | Allow a non-loopback `--listen` without a database. Anyone who can reach the port can read everything. |
| `--allow-publish` | `RMF_PIPES_ALLOW_PUBLISH` | — | Topics that may be published to via the API, comma-separated `pattern` or `pattern=admin` (default role: operator), e.g. `door_requests,fleet/*/estop=admin`. Needs `--database-url` (auth) or `--demo`. |
| `--allow-call` | `RMF_PIPES_ALLOW_CALL` | — | Services that may be called via the API, same syntax. |
| `--write-interval` | `RMF_PIPES_WRITE_INTERVAL` | `200ms` | Minimum time between writes by one user to one topic or service (`0s` = no limit). |
| `--discovery-timeout` | `RMF_PIPES_DISCOVERY_TIMEOUT` | `5s` | How long to wait for each node asked for a type description. |
| `--record-config` | `RMF_PIPES_RECORD_CONFIG` | — | Recording rules to start with: a TOML file of `[[rules]]` tables (see [recording.md](recording.md)). Invalid rules stop startup with the reason. Rules can be replaced at runtime through the API; the file is never rewritten. |
| `--record-retention` | `RMF_PIPES_RECORD_RETENTION` | `7d` | Stored records older than this are deleted, at startup and hourly. `0s` keeps them forever. |
| `--max-record-streams` | `RMF_PIPES_MAX_RECORD_STREAMS` | `256` | Most recording streams at once (1–10000); further matches are skipped and reported. |

## Users
`rmf-pipes user …` manages users directly in the database (needs `--database-url`). Passwords are read from the first line of stdin:
```
rmf-pipes user add alice --role admin < alice.pw
rmf-pipes user list
rmf-pipes user set-password alice < new.pw     # ends alice's sessions and revokes her tokens
rmf-pipes user set-role bob operator
rmf-pipes user disable bob
```
Roles: `viewer` (read), `operator` (read, and write to allowlisted topics and services), `admin` (everything, including users, every token, the audit log and the recording rules). Scripts authenticate with API tokens: create one with `POST /api/v1/auth/tokens`, then send `Authorization: Bearer rp_…`.

## Startup behaviour
- rmf-pipes joins zenoh **before** it logs `listening`. In client mode it **exits with an error** if no router is reachable, rather than serving an empty graph.
- hiroz also reads `ZENOH_CONFIG_OVERRIDE` and `ZENOH_SESSION_CONFIG_URI` directly. They work, but prefer `--zenoh-config`, so that the whole configuration is visible in one place.

## Modes at a glance
| | No `--database-url` | `--database-url` | `--demo` (± `--database-url`) |
|---|---|---|---|
| Login | none (anonymous viewer) | required | none (everyone is an admin) |
| Writes, rule changes | refused (`503 auth_disabled` / `403`) | by role | allowed (allowlist and rate limit still apply) |
| Audit log | — | database | database, else the process log |
| Recording sinks | console | console, database | console, database if configured |
| `--listen` | loopback (or `--insecure-no-auth`) | any | loopback only |

## Topics need a type description
rmf-pipes learns each message definition from a node's `~/get_type_description` service. It asks the topic's publishers (or the service's servers) of the chosen type first. When a topic carries several types, it picks the one with the most publishers. Then it asks up to 3 other nodes that use the same type and hash anywhere in the graph, because descriptions are per type, not per topic. Successful answers are cached by (type, hash). The service is on by default for rclcpp and rclpy nodes on Jazzy and later.

A type that no node describes shows up in the graph but can't be decoded: its status is `failed`, with a schema error. Nodes that don't answer include:
- nodes that sleep without spinning, like `ros2 topic pub --once`
- rclrs nodes

Either works once any other node uses the same type.
