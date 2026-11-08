//! L3 end to end: the real binary, an in-process zenoh router, and a hiroz
//! fixture publisher, exercised only over HTTP the way a client would.

#![allow(clippy::unwrap_used)]

use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use futures::StreamExt;
use serde_json::{Value, json};

const BIN: &str = env!("CARGO_BIN_EXE_rmf-pipes");

struct Running {
    child: Child,
    base: String,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(router: &pipes_testkit::TestRouter, extra: &[&str]) -> Running {
    let mut child = Command::new(BIN)
        .env_clear()
        .env("RUST_LOG", "info")
        .args(["--listen", "127.0.0.1:0", "--log-format", "json"])
        .args(["--zenoh-connect", &router.endpoint()])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(v) = serde_json::from_str::<Value>(&line)
                && v["message"] == "listening"
            {
                let _ = tx.send(v["addr"].as_str().unwrap().to_owned());
            }
        }
    });
    let addr = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("never logged `listening`");
    Running {
        child,
        base: format!("http://{addr}"),
    }
}

async fn get_json(url: &str) -> (u16, Value) {
    let res = reqwest::get(url).await.unwrap();
    let status = res.status().as_u16();
    (status, res.json().await.unwrap())
}

/// Poll `url` until `ok(json)` holds.
async fn poll_json(url: &str, what: &str, ok: impl Fn(u16, &Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (status, v) = get_json(url).await;
        if ok(status, &v) {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; last: {status} {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_sees_the_graph_latest_samples_and_a_live_stream() {
    let router = pipes_testkit::TestRouter::start();
    let _talker = pipes_testkit::spawn_string_publisher(
        &router.endpoint(),
        "talker",
        "/chatter",
        Duration::from_millis(50),
        |i| format!("hello {i}"),
    );
    let app = start(&router, &[]);

    // Graph.
    poll_json(
        &format!("{}/api/v1/graph", app.base),
        "/chatter in the graph",
        |s, g| {
            s == 200
                && g["topics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|t| t["name"] == "/chatter")
        },
    )
    .await;

    // Latest, subscribing on demand.
    let (status, sample) = get_json(&format!(
        "{}/api/v1/topic/latest?name=/chatter&wait_ms=5000",
        app.base
    ))
    .await;
    assert_eq!(status, 200, "{sample}");
    assert_eq!(sample["type_name"], "std_msgs/msg/String");
    assert!(
        sample["data"]["data"]
            .as_str()
            .unwrap()
            .starts_with("hello ")
    );
    assert!(sample["sequence"].is_number());

    // Schema.
    let (status, schema) =
        get_json(&format!("{}/api/v1/topic/schema?name=/chatter", app.base)).await;
    assert_eq!(status, 200);
    assert_eq!(schema["fields"][0]["name"], "data");

    // Live stream: two consecutive samples with increasing sequence numbers.
    let res = reqwest::get(format!("{}/api/v1/topic/stream?name=/chatter", app.base))
        .await
        .unwrap();
    assert_eq!(res.headers()["content-type"], "text/event-stream");
    let mut body = res.bytes_stream();
    let mut buf = String::new();
    let mut seqs = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seqs.len() < 2 {
        assert!(Instant::now() < deadline, "no live samples: {buf}");
        let chunk = tokio::time::timeout(Duration::from_secs(5), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        buf.push_str(std::str::from_utf8(&chunk).unwrap());
        while let Some(end) = buf.find("\n\n") {
            let block: String = buf.drain(..end + 2).collect();
            if block.contains("event: sample") {
                let data = block
                    .lines()
                    .find_map(|l| l.strip_prefix("data: "))
                    .unwrap();
                let v: Value = serde_json::from_str(data).unwrap();
                seqs.push(v["sequence"].as_i64().unwrap());
            }
        }
    }
    assert!(
        seqs[1] > seqs[0],
        "sequence numbers should increase: {seqs:?}"
    );

    // The dashboard lists the topic, and the docs are served.
    let overview = reqwest::get(format!("{}/ui/fragments/overview", app.base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(overview.contains("/chatter"));
    let docs = reqwest::get(format!("{}/docs/", app.base)).await.unwrap();
    assert_eq!(docs.status(), 200);
    let (status, doc) = get_json(&format!("{}/openapi.json", app.base)).await;
    assert_eq!(status, 200);
    assert!(doc["paths"]["/api/v1/topic/latest"].is_object());

    // Metrics count what was delivered.
    let metrics = reqwest::get(format!("{}/metrics", app.base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics
            .lines()
            .any(|l| l.starts_with("rmf_pipes_samples_delivered_total{topic=\"/chatter\"}")),
        "{metrics}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standing_patterns_are_served_without_a_client_asking_first() {
    let router = pipes_testkit::TestRouter::start();
    let _adapter = pipes_testkit::spawn_string_publisher(
        &router.endpoint(),
        "fleet_adapter",
        "/fleet/r1/state",
        Duration::from_millis(50),
        |_| "idle".to_owned(),
    );
    let app = start(&router, &["--ingest", "fleet/*/state"]);
    let topics = poll_json(
        &format!("{}/api/v1/topics", app.base),
        "a standing subscription",
        |s, v| {
            s == 200
                && v.as_array().unwrap().iter().any(|t| {
                    t["name"] == "/fleet/r1/state"
                        && t["standing"] == true
                        && t["status"]["state"] == "active"
                })
        },
    )
    .await;
    let t = topics
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "/fleet/r1/state")
        .unwrap();
    assert_eq!(t["watchers"], 0, "nobody asked; it is standing");
    let (_, sample) = get_json(&format!(
        "{}/api/v1/topic/latest?name=/fleet/r1/state",
        app.base
    ))
    .await;
    assert_eq!(sample["data"], json!({"data": "idle"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_with_an_sse_client_connected_drains_promptly() {
    let router = pipes_testkit::TestRouter::start();
    let _talker = pipes_testkit::spawn_string_publisher(
        &router.endpoint(),
        "talker",
        "/chatter",
        Duration::from_millis(50),
        |i| format!("hello {i}"),
    );
    let mut app = start(&router, &["--shutdown-drain", "10s"]);
    let res = reqwest::get(format!("{}/api/v1/topic/stream?name=/chatter", app.base))
        .await
        .unwrap();
    let mut body = res.bytes_stream();
    // Wait until the stream is live (first event received).
    let first = tokio::time::timeout(Duration::from_secs(10), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(&first)
            .unwrap()
            .contains("event: status")
    );

    let started = Instant::now();
    let status = Command::new("kill")
        .args(["-TERM", &app.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    // The client sees a final closed status and then the end of the stream.
    let mut tail = String::new();
    while let Ok(Some(chunk)) = tokio::time::timeout(Duration::from_secs(5), body.next()).await {
        tail.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
    }
    assert!(
        tail.contains(r#"data: {"state":"closed"}"#),
        "no final closed event: {tail}"
    );
    // And the server exits well before the 10s drain deadline.
    let exited = loop {
        if let Some(s) = app.child.try_wait().unwrap() {
            break s;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "server did not exit promptly"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(exited.success());
}
