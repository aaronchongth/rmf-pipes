# Demo: recording Nav2 navigation with rmf-pipes

This demo runs the whole stack on one machine:
- **[next_gen_prototype](https://github.com/open-rmf/next_gen_prototype)'s Nav2 demo** in Docker, on ROS 2 Jazzy with rmw_zenoh: Gazebo, two TurtleBot3s (`robot0`, `robot1`) each running Nav2, and the prototype's path server, plan executor and `rmf_nav2_traffic` nodes, all talking through one zenoh router.
- **rmf-pipes** on the host, joined to that router. It observes each robot's `navigate_to_pose` action and records the goals, status changes, results and feedback to the console and to Postgres.

Authentication is off: rmf-pipes runs in **demo mode** (`--demo`), which only listens on `127.0.0.1`.

```
 Docker (pipes-nav2)                                        host
┌──────────────────────────────────────────────┐
│ gazebo_server   robot0_nav2_sim   robot1_nav2_sim │
│ next_gen_prototype (rmf_nav2_traffic, …)     │
│        └──────── zenoh_router ───────────────┼── 127.0.0.1:7447 ── rmf-pipes ── 127.0.0.1:18080 (API, dashboard)
└──────────────────────────────────────────────┘                       └─ Postgres (pixi, 127.0.0.1:54329)
```

## Prerequisites
- Docker, usable without sudo (you're in the `docker` group).
- A checkout of `next_gen_prototype` next to this repo (`../next_gen_prototype`), or set `NEXT_GEN_PROTOTYPE=/path/to/it`. The demo doesn't modify it: `compose.override.yml` here is layered on its compose files.
- Rust (rustup) and pixi, as for the rest of rmf-pipes. Pixi provides Postgres.
- About 32 GB of disk for the prototype's image. The first build takes a while (around 15 minutes here).

## Run it
```
pixi run demo-up      # build the prototype image (first run only) and start the Nav2 stack
pixi run demo         # start rmf-pipes in demo mode (foreground; Ctrl-C stops it)
```
The first `pixi run demo` compiles rmf-pipes for Jazzy into `target/demo-jazzy`. Nav2 takes about a minute to become active after `demo-up`.

`pixi run demo` starts:
- the local Postgres, with a `rmf_pipes_demo` database
- rmf-pipes with `--demo`, `--database-url`, and `--record-config demo/nav2/record.toml`, connected to `tcp/127.0.0.1:7447` on ROS domain 0

Then, in another terminal:
```
pixi run demo-goal robot1 9.0 6.0     # send robot1 across the map (blocks until it arrives)
pixi run demo-goal robot0 5.0 5.0
pixi run demo-check                   # automated end-to-end check (below)
```
`demo-goal` runs `ros2 action send_goal robotN/navigate_to_pose …` inside the `next_gen_prototype` container. rmf-pipes is not in the path: it observes the goal on the bus.

Stop everything with Ctrl-C in the `pixi run demo` terminal, then `pixi run demo-down`.

## What to look at
1. **The console** of `pixi run demo` shows one line per record from the `nav-goals` rule: the goal (with its target pose), `status` changes (`executing`, `succeeded`), and the `result`:
   ```
   INFO rmf_pipes::record: record rule=nav-goals kind="action" name=/robot1/navigate_to_pose part="goal" goal_id="a2449533-…" type_name=nav2_msgs/action/NavigateToPose data={"pose":{"header":{"stamp":{"sec":0,"nanosec":0},"frame_id":"map"},"pose":{"position":{"x":9.0,"y":6.0,"z":0.0},…}},"behavior_tree":""}
   INFO rmf_pipes::record: record rule=nav-goals … part="status" … data={"status":"executing","code":2}
   INFO rmf_pipes::record: record rule=nav-goals … part="status" … data={"status":"succeeded","code":4}
   INFO rmf_pipes::record: record rule=nav-goals … part="result" … data={"status":"succeeded","result":{"error_code":0,"error_msg":""}}
   ```
   `goal_id` is the ID `ros2 action send_goal` printed, with dashes.
2. **The dashboard**, at <http://127.0.0.1:18080/ui/recording>, shows:
   - the rules, in an editor
   - every stream: what it observes, whether it's active, and recorded and dropped counts
   - the latest records

   The **Graph** page lists everything else on the bus (about 50 nodes and 260 topics). None of it is recorded unless a rule asks for it.
3. **The database**:
   ```
   curl -s 'http://127.0.0.1:18080/api/v1/records?part=result' | python3 -m json.tool
   pixi run psql -h 127.0.0.1 -p 54329 -U postgres rmf_pipes_demo \
     -c "SELECT at, name, part, data FROM pipes.records WHERE goal_id = '<uuid>' ORDER BY at"
   ```
   Feedback (`nav-feedback` rule) goes only to the database, at most once a second per robot. Nav2 sends about 20 a second; the rest show as `dropped` on the dashboard.
4. **Swagger UI**, at <http://127.0.0.1:18080/docs/>, under *recording*, has every endpoint ready to try.

## Change what's recorded, live
The rules in [`record.toml`](record.toml) are only the starting point. Replace them at runtime from the dashboard (edit, then **Save rules**), from Swagger UI, or with curl. For example, also record the goals Nav2 itself receives from the prototype's traffic node (`…/inner/navigate_to_pose`), and stop recording feedback:
```
curl -s -X PUT http://127.0.0.1:18080/api/v1/recording/rules \
  -H 'Content-Type: application/json' -d '{"rules": [
    {"name": "nav-goals", "match": "/*/navigate_to_pose", "kind": "action",
     "parts": ["goal", "status", "result"], "sinks": ["console", "database"]},
    {"name": "nav2-inner", "match": "/*/inner/navigate_to_pose", "kind": "action",
     "parts": ["goal", "result"], "sinks": ["console"]}
  ]}'
```
- The unchanged `nav-goals` rule keeps its streams.
- `nav2-inner` starts within a second.
- `nav-feedback` stops.

Send another goal and both the outer goal and the one the traffic node forwards to Nav2 appear. Other ideas:
- `{"name": "odom", "match": "/*/inner/odom", "kind": "topic", "sinks": ["database"], "max_rate_hz": 2}`: robot odometry, twice a second.
- `{"name": "paths", "match": "/*/inner/follow_path", "kind": "action", "parts": ["goal"], "sinks": ["database"]}`: every path Nav2's controller is asked to follow. These are large, which is fine for the database. The console cuts payloads at 2 KiB.

`Content-Type: application/json` is required. In demo mode rmf-pipes refuses other non-GET requests, and any request not addressed to `localhost` or `127.0.0.1`, so a web page in your browser can't drive it.

Changes last until rmf-pipes restarts. Edit `record.toml` to keep them. The format is in [docs/recording.md](../../docs/recording.md).

## The automated check
`pixi run demo-check` needs the stack and `pixi run demo` running. It:
1. waits until robot0's recording streams are active
2. sends robot0 a goal to (1.0, 5.0) and reads the goal ID the client was given
3. polls `GET /api/v1/records?goal_id=…` until that goal's `goal`, `status` and `result` records are all stored

It prints `OK` or fails with what's missing. It isn't part of `pixi run ci`, which stays hermetic. `pixi run interop` covers the same path against a small rclpy action server.

## Troubleshooting
- **`cannot talk to Docker`**: start Docker, and log out and back in after adding yourself to the `docker` group.
- **No streams on the dashboard**: Nav2 is still starting, or the router isn't reachable. `curl -s localhost:18080/api/v1/graph | head` should list the robots' nodes.
- **Port 7447 or 18080 in use**: set `PIPES_DEMO_ZENOH_PORT` or `PIPES_DEMO_LISTEN` for `demo-up` and `demo` alike.
- **`ROS_DOMAIN_ID` in your shell**: doesn't matter. The demo passes `--domain-id 0` to match the containers.
- **Robots don't move**: `docker logs robot0_nav2_sim`. Nav2 needs its localization to come up, which happens about a minute after `demo-up`.
