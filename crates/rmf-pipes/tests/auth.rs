//! End-to-end auth through the real binary: the anonymous-mode guard, secret
//! hygiene, the `user` CLI, and bootstrap + login over HTTP. Database tests
//! need `pixi run test` (which provides `DATABASE_URL`).

#![allow(clippy::unwrap_used)] // test helpers outside #[test] fns

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Output, Stdio},
    time::Duration,
};

use serde_json::{Value, json};
use sqlx::PgPool;

const BIN: &str = env!("CARGO_BIN_EXE_rmf-pipes");
const PW: &str = "correct horse battery";

/// The URL of the database `#[sqlx::test]` created for this test.
fn db_url(pool: &PgPool) -> String {
    let base = std::env::var("DATABASE_URL").expect("DATABASE_URL (run via `pixi run test`)");
    let db = pool.connect_options().get_database().unwrap().to_owned();
    let (prefix, _) = base.rsplit_once('/').unwrap();
    format!("{prefix}/{db}")
}

fn cli(args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(BIN)
        .env_clear()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn text(o: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

#[test]
fn refuses_to_serve_unauthenticated_on_the_network() {
    let out = cli(&["--listen", "0.0.0.0:0"], "");
    assert!(!out.status.success());
    let t = text(&out);
    assert!(
        t.contains("refusing to serve without authentication"),
        "{t}"
    );
    assert!(!t.contains("\"listening\""));
}

#[test]
fn a_bad_database_url_never_leaks_its_password() {
    let router = pipes_testkit::TestRouter::start();
    let secret = "pa55-w0rd-that-must-not-leak";
    let out = cli(
        &[
            "--listen",
            "127.0.0.1:0",
            "--zenoh-connect",
            &router.endpoint(),
            "--database-connect-timeout",
            "300ms",
            "--database-url",
            &format!("postgres://rmf:{secret}@127.0.0.1:1/pipes"),
        ],
        "",
    );
    assert!(!out.status.success());
    let t = text(&out);
    assert!(t.contains("cannot use the database"), "{t}");
    assert!(!t.contains(secret), "leaked the password: {t}");
    let out = cli(
        &[
            "user",
            "list",
            "--database-url",
            &format!("not-a-url-{secret}"),
        ],
        "",
    );
    assert!(!out.status.success());
    assert!(!text(&out).contains(secret), "{}", text(&out));
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn the_user_cli_manages_users(pool: PgPool) {
    let url = db_url(&pool);
    let run = |args: &[&str], stdin: &str| {
        let mut all = vec!["user"];
        all.extend_from_slice(args);
        all.extend_from_slice(&["--database-url", &url]);
        cli(&all, stdin)
    };
    let out = run(&["add", "alice", "--role", "admin"], &format!("{PW}\n"));
    assert!(out.status.success(), "{}", text(&out));
    assert!(text(&out).contains("created alice (admin)"));
    let out = run(&["add", "bob"], "short\n");
    assert!(!out.status.success());
    assert!(
        text(&out).contains("at least 10 characters"),
        "{}",
        text(&out)
    );
    let out = run(&["add", "bob"], "");
    assert!(
        text(&out).contains("no password on stdin"),
        "{}",
        text(&out)
    );
    run(&["add", "bob", "--role", "operator"], PW);
    let out = run(&["list"], "");
    let listing = text(&out);
    assert!(
        listing.contains("alice") && listing.contains("bob") && listing.contains("operator"),
        "{listing}"
    );
    let out = run(&["set-role", "alice", "viewer"], "");
    assert!(
        !out.status.success() && text(&out).contains("last enabled admin"),
        "{}",
        text(&out)
    );
    assert!(run(&["disable", "bob"], "").status.success());
    assert!(
        run(&["set-password", "bob"], "another long password\n")
            .status
            .success()
    );
    assert!(run(&["delete", "bob"], "").status.success());
    let db = pipes_db::Db::from_pool(pool);
    let actions: Vec<String> = db
        .list_audit(&pipes_db::AuditQuery::default())
        .await
        .unwrap()
        .into_iter()
        .map(|r| format!("{}:{}:{}", r.actor, r.action, r.target))
        .collect();
    assert_eq!(
        actions,
        vec![
            "cli:user.delete:bob",
            "cli:user.update:bob",
            "cli:user.update:bob",
            "cli:user.create:bob",
            "cli:user.create:alice"
        ]
    );
}

struct Server {
    child: Child,
    base: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn serve(router: &pipes_testkit::TestRouter, extra: &[&str], env: &[(&str, &str)]) -> Server {
    let mut child = Command::new(BIN)
        .env_clear()
        .env("RUST_LOG", "info")
        .envs(env.iter().copied())
        .args([
            "--listen",
            "127.0.0.1:0",
            "--log-format",
            "json",
            "--zenoh-connect",
            &router.endpoint(),
        ])
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
        .recv_timeout(Duration::from_secs(30))
        .expect("never logged `listening`");
    Server {
        child,
        base: format!("http://{addr}"),
    }
}

#[sqlx::test(migrator = "pipes_db::MIGRATOR")]
async fn bootstrap_admin_logs_in_and_the_api_is_protected(pool: PgPool) {
    let url = db_url(&pool);
    // `#[sqlx::test]` runs a current-thread runtime, which zenoh refuses; open
    // the router outside it.
    let router = std::thread::spawn(pipes_testkit::TestRouter::start)
        .join()
        .unwrap();
    let app = serve(
        &router,
        &["--database-url", &url, "--bootstrap-admin", "root"],
        &[("RMF_PIPES_BOOTSTRAP_PASSWORD", PW)],
    );
    let http = reqwest::Client::new();
    let r = http
        .get(format!("{}/api/v1/graph", app.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    assert_eq!(
        http.get(format!("{}/healthz", app.base))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let r = http
        .post(format!("{}/api/v1/auth/login", app.base))
        .json(&json!({"username": "root", "password": PW}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let cookie = r.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let me: Value = http
        .get(format!("{}/api/v1/auth/me", app.base))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (me["user"]["username"].as_str(), me["role"].as_str()),
        (Some("root"), Some("admin"))
    );
    let t: Value = http
        .post(format!("{}/api/v1/auth/tokens", app.base))
        .header("cookie", &cookie)
        .json(&json!({"name": "e2e"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = t["token"].as_str().unwrap();
    let r = http
        .get(format!("{}/api/v1/graph", app.base))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // A second start doesn't re-bootstrap or fail.
    drop(app);
    let again = serve(
        &router,
        &["--database-url", &url, "--bootstrap-admin", "someone_else"],
        &[],
    );
    let r = http
        .post(format!("{}/api/v1/auth/login", again.base))
        .json(&json!({"username": "root", "password": PW}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let db = pipes_db::Db::from_pool(pool);
    assert!(db.user_by_name("someone_else").await.unwrap().is_none());
    drop(again);
    // Close the router outside the current-thread runtime too.
    std::thread::spawn(move || drop(router)).join().unwrap();
}
