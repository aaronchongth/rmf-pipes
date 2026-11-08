# Decisions

These are settled decisions, newest first. Reopen one only with new information, and record the change here.

## 2026-11-09: Writes are allowlisted, rate-limited and audited before they happen
- `POST /api/v1/topic/publish` and `POST /api/v1/service/call` need authentication; anonymous mode is read-only.
- The target must match an `--allow-publish` / `--allow-call` pattern. The pattern sets the minimum role (operator by default, `=admin` to restrict). When several match, the most permissive applies. An empty allowlist means nothing can be written.
- Writes are limited to one per user per target per `--write-interval` (200 ms by default).
- **Fail closed, and completed**:
  - A `pending` entry, with the payload clipped at 4 KiB (`payload_truncated` sits outside the payload, so it can't be forged), is written before the bus is touched. If it can't be written, the write is refused (`503 audit_unavailable`).
  - The write then runs in its own task, so a client disconnecting can't cut it short, and an `ok` entry (with the type and hash used) or an `error` entry, linked by `intent`, records how it ended.
- **Bounded**:
  - Names are at most 255 bytes.
  - Refusals are audited at most 20 per user per action per minute.
  - At most 32 writes run at once.
  - A message may build at most 100 000 elements.
  - Service calls wait at most 9 s, because hiroz's querier gives up at 10 s by itself.
- **The message type is taken from the topic's subscribers** (else its publishers), and publishes bind to their advertised hash, like subscriptions. So something must already use a topic before rmf-pipes can write to it, and there's no free-form type parameter to get wrong.
  - If the subscribers (or publishers) disagree on the type, the write is refused (`409 type_conflict`) rather than leaving some of them without it.
  - A service with more than one server is refused too, because a zenoh query would run on all of them.
  - A type description is only accepted if it describes the type that was asked for.
  - Omitted fields take the message's own defaults from its type description (e.g. `Quaternion.w = 1.0`).
  - rmf-pipes' own publishes don't make a topic "published" for `/latest`; its write publishers are undeclared after 60 s unused.
- **JSON → message is strict.** Unknown fields, wrong types, out-of-range numbers and bounds violations are `400 invalid_message`, with the field path. Omitted fields take their defaults. 64-bit integers accept numbers or strings, and byte arrays accept base64 or arrays.
- **Service calls** use hiroz's public typed client with a pass-through CDR type (`pipes-bus/src/raw.rs`), because the raw client is FFI-only.
  - Request and response schemas come from the server's type description of the service type (rclcpp/rclpy), falling back to separate request/response descriptions (hiroz servers).
  - Service part types (`pkg/srv/X_Request`) are renamed to `pkg/msg/…` internally, because hiroz's schema builder only accepts message names.

## 2026-11-09: Auth is built in; without a database the API is anonymous and read-only
- **Users, roles, sessions, API tokens and the audit log live in Postgres** (`pipes-db`, schema `pipes`; sqlx's own `_sqlx_migrations` table is in `public`).
  - Roles are `viewer` < `operator` < `admin`.
  - Passwords are argon2id (`password-auth`).
  - Sessions and tokens are 256-bit random values; only their SHA-256 is stored. Tokens carry an `rp_` prefix.
  - The last enabled admin can't be demoted, disabled or deleted.
- **Sessions** are `HttpOnly; SameSite=Strict` cookies with a sliding idle timeout.
- **CSRF**: cookie-authenticated non-GET requests must be `application/json`.
- **Bearer tokens** take precedence over cookies: a bad token is a 401, never a silent fallback.
- **Login limits**: failed password checks (logins and password changes) are counted in a sliding window.
  - The (username, client address) pair gets 5 per 15 minutes, and each address 50.
  - There is deliberately **no per-username limit across addresses**, because that would let anyone lock a user out. The trade-off is 5 guesses per address per window against argon2id hashes of passwords with at least 10 characters.
  - Only the first refusal of each block is audited, and malformed usernames are refused before touching the database, the limiter or the audit log, so unauthenticated callers can't flood either.
  - Behind a reverse proxy the address is the proxy's; `X-Forwarded-For` is deliberately not trusted.
- **Account changes are atomic and serialised.** Every user change runs in one transaction with its audit entry, under an advisory lock, so concurrent requests can't remove the last enabled admin and a rejected request changes nothing.
- **Recovery resets revoke tokens.** An admin or CLI password reset also revokes the user's API tokens; a self-service change keeps them. A token can't mint a token that outlives it. Token lifetimes are 1–3650 days, or unlimited.
- **Sessions** also have an absolute lifetime (`--session-max`). Logging in again retires the session the browser came with. Known limitation: an open SSE stream keeps running after its session is revoked, until the client disconnects or the server restarts.
- **Without `--database-url`**, everyone is an anonymous viewer, account and write endpoints answer `503 auth_disabled`, and the binary refuses a non-loopback `--listen` unless `--insecure-no-auth` is given. This keeps the no-dependency dev loop and the existing tests.
- **Queries are runtime-checked** (`sqlx::query`), not compile-time macros. Every query is exercised by a `#[sqlx::test]` against a fresh database, which avoids the `.sqlx` offline-cache workflow.
- **OIDC is deferred.** The built-in path covers air-gapped sites. Sites with an identity provider can wait for the OIDC follow-up.

## 2026-11-08: Bounded on-demand topics and SSE streams
Any client can ask for any topic name. Without limits, that means unbounded subscriptions and retry tasks. On-demand topics (`--max-topics`) and SSE streams (`--max-streams`) are capped, and the excess gets `503`. Standing topics are configured by the operator, so they don't count toward the cap.

## 2026-11-08: We convert type descriptions ourselves
hiroz 0.2.1's `discover_topic_schema` rejects `byte`, `char`, bounded and fixed strings ("Unknown type ID: 16/21"). That rules out `diagnostic_msgs`, `/parameter_events` and anything with `byte[]`. `pipes-bus/src/type_desc.rs` calls the standard `~/get_type_description` service through hiroz's public `GetTypeDescription` types and maps every ROS type id. Wide strings and `long double` stay unsupported because hiroz can't decode them. **Remove `type_desc.rs` once hiroz maps every id**; the fix should go upstream.

## 2026-11-08: The rate cap keeps the newest sample
A leading-edge throttle dropped the last sample of a burst for good: a door that changed state three times quickly showed the first state forever. The cap is trailing-edge: within each interval the newest sample replaces any pending one, and it is delivered when the interval ends.

## 2026-11-08: Subscriptions bind to the publisher's advertised hash
`pipes-bus` subscribes through hiroz's doc-hidden `create_sub_impl` with the publisher's `TypeInfo`. The public `create_dyn_sub` recomputes the hash locally, and rmw_zenoh routes by hash, so any difference would silently deliver nothing. The test `subscription_binds_to_the_advertised_hash` guards this: its fixture advertises a hash hiroz would never compute and serves its own type description. Swapping in `create_dyn_sub` makes it fail.

## 2026-11-08: Fail fast when no zenoh router is reachable
In client mode, startup exits with an error before `listening`, rather than serving an empty graph. Restarting is the orchestrator's job, whether that's systemd or k8s.

## 2026-11-08: `ROS_DOMAIN_ID` is honoured
hiroz 0.2.1 reads `ROS_DOMAIN_ID`; the Sept 2026 spike, on an older hiroz, saw it ignored. rmf-pipes exposes the domain as `--domain-id`, with `ROS_DOMAIN_ID` as its env var. Tests pin domain 0 explicitly.

## 2026-11-08: Defer Postgres to phase 3
Nothing in phases 0–2 persists anything: live state is in memory. Postgres (sqlx, migrations, `sqlx::test`, pixi-managed `postgresql`) arrives with auth in phase 3. Until then, the server runs with no external dependencies apart from a zenoh router, which keeps the agent inner loop trivial.

## 2026-11-08: Wildcards are matched on the ROS graph, and hiroz owns the subscriptions
Next-gen topic taxonomies need wildcard subscriptions, such as `fleet/*/state`. rmf-pipes does **not** subscribe to raw zenoh key expressions. Instead:
1. Patterns are zenoh key-expression syntax applied to ROS topic names, without the leading `/`.
2. They are matched against topics in hiroz's liveliness-based graph.
3. Each match gets its own hiroz dynamic subscription, built with the publisher's QoS.

This keeps rmw_zenoh specifics inside hiroz: key mangling, type hashes, transient_local history, and the `_buf_cpu` buffer groups. It also keeps unmatched high-rate traffic off the server.

## 2026-11-08: Messages are decoded at runtime, and we own the JSON mapping
Topics are decoded with hiroz `dynamic` messages. The schema is fetched from the publishing node's `~/get_type_description` service, so there's no per-type codegen. hiroz has no JSON form for dynamic messages, so rmf-pipes defines one in `docs/json-mapping.md`. **That mapping is a public contract.**

## 2026-11-08: Default distro is Lyrical, one build per distro
hiroz selects the rmw_zenoh conventions with mutually exclusive compile-time features. rmf-pipes forwards `jazzy | kilted | lyrical | rolling` and refuses to build with more than one. The default is Lyrical, the newest LTS: it's the distro installed for local interop tests, and Open-RMF has a Lyrical release. Humble isn't supported (no type hashes). Use `hiroz-bridge` if you ever need it.

## 2026-11-08: Pin hiroz 0.2.1 and zenoh 1.9.0 exactly
hiroz is v0.x, so it's allowed to break between minors. It requires `zenoh ^1.9.0`, but it's built and tested against 1.9.0, and zenoh 1.10 is already on crates.io. Both crates are pinned with `=`. Upgrades happen in their own commit and must pass the interop tests. **Watch Zenoh 2.0** (planned for H2 2026; 1.x compatibility hasn't been announced).

## 2026-11-08: Keep an API server between the web and the robot bus
The browser and web clients never speak zenoh. The API server:
- is the rate, validation and authorization boundary
- keeps DB stalls and plugin crashes out of the router that carries robot traffic
- gives end-user identity (zenoh ACL only identifies sessions)
- fits normal web deployment

## 2026-11-08: Auth is built in, Grafana-style
This arrives in phase 3: local users, optional OIDC, sessions and hashed API tokens. We don't bundle an identity provider. **Grafana isn't an identity provider.** Its role is an optional metrics and history UI, reading `/metrics` and the Postgres storage tables.

## 2026-11-08: Postgres-first zenoh storage backend, prototyped in this repo
It will live in `crates/zenoh-backend-postgres` (phase 5) with no rmf-pipes dependencies, licensed so it can move to open-rmf or eclipse-zenoh once stable. Postgres is the "boring, reliable SQL", and it covers Supabase/Neon/RDS for free. SQLite may follow as a second dialect.

## 2026-11-08: Minimal dashboard, OpenAPI for everything else
The dashboard only verifies that the data layer works (graph, topics, live samples, mismatches, metrics). There are no operational or domain features. The API is described by OpenAPI (utoipa) with a vendored Swagger UI at `/docs`. **No Node toolchain.**

## Inherited from rmf-web modernization / rmf-console (Sept 2026)
- rmw_zenoh is the only supported RMW.
- Postgres via sqlx, no ORM.
- Tests are hermetic by default, with a nightly interop check against real rmw_zenoh.
- Lessons kept:
  - `--help` must not leak secrets
  - `/healthz` must not leak raw DB errors
  - shutdown needs a drain deadline
  - message type-hash mismatches cause **silent drops**, so detect them at runtime
