//! `rmf-pipes user …`: user management from the command line, e.g. to create
//! the first admin or reset a password when nobody can log in. Every change is
//! audited with actor `cli`.

use std::io::{BufRead, IsTerminal, Write};

use anyhow::{Context, bail};
use pipes_db::{AuditEntry, Db, Outcome, Role, UserChanges};
use serde_json::json;

use crate::config::{Config, UserAction};

/// An audit entry for an action taken from the command line.
pub fn cli_audit(action: &str, target: &str, detail: serde_json::Value) -> AuditEntry {
    AuditEntry {
        actor_id: None,
        actor: "cli".into(),
        auth_method: "cli".into(),
        action: action.into(),
        target: target.into(),
        outcome: Outcome::Ok,
        detail,
    }
}

/// Run a user subcommand.
pub async fn run(config: &Config, action: &UserAction) -> anyhow::Result<()> {
    let url = config
        .database_url
        .as_deref()
        .context("`rmf-pipes user` needs --database-url (or RMF_PIPES_DATABASE_URL)")?;
    let db = Db::connect(url, 2, config.database_connect_timeout).await?;
    let out = std::io::stdout();
    let mut out = out.lock();
    match action {
        UserAction::Add { username, role } => {
            let password = read_password()?;
            let user = db.create_user(username, &password, (*role).into()).await?;
            db.record_audit(&cli_audit(
                "user.create",
                &user.username,
                json!({"role": user.role}),
            ))
            .await?;
            writeln!(out, "created {} ({})", user.username, user.role)?;
        }
        UserAction::List => {
            let users = db.list_users().await?;
            writeln!(
                out,
                "{:<24} {:<9} {:<9} created",
                "username", "role", "state"
            )?;
            for u in users {
                let state = if u.disabled { "disabled" } else { "enabled" };
                writeln!(
                    out,
                    "{:<24} {:<9} {:<9} {}",
                    u.username,
                    u.role,
                    state,
                    u.created_at.date()
                )?;
            }
        }
        UserAction::SetPassword { username } => {
            // A CLI reset is account recovery: also revoke the user's tokens.
            let password = read_password()?;
            let changes = UserChanges {
                password: Some(password),
                revoke_tokens: true,
                ..UserChanges::default()
            };
            let entry = cli_audit(
                "user.update",
                username,
                json!({"password_changed": true, "tokens_revoked": true}),
            );
            let user = db.change_user(username, &changes, Some(&entry)).await?;
            writeln!(
                out,
                "password of {} changed; their sessions ended and API tokens were revoked",
                user.username
            )?;
        }
        UserAction::SetRole { username, role } => {
            let role: Role = (*role).into();
            let changes = UserChanges {
                role: Some(role),
                ..UserChanges::default()
            };
            let entry = cli_audit("user.update", username, json!({"role": role}));
            let user = db.change_user(username, &changes, Some(&entry)).await?;
            writeln!(out, "{} is now {}", user.username, user.role)?;
        }
        UserAction::Disable { username } | UserAction::Enable { username } => {
            let disabled = matches!(action, UserAction::Disable { .. });
            let changes = UserChanges {
                disabled: Some(disabled),
                ..UserChanges::default()
            };
            let entry = cli_audit("user.update", username, json!({"disabled": disabled}));
            let user = db.change_user(username, &changes, Some(&entry)).await?;
            writeln!(
                out,
                "{} is {}",
                user.username,
                if disabled { "disabled" } else { "enabled" }
            )?;
        }
        UserAction::Delete { username } => {
            db.delete_user(
                username,
                Some(&cli_audit("user.delete", username, json!({}))),
            )
            .await?;
            writeln!(out, "deleted {username}")?;
        }
    }
    Ok(())
}

/// The first line of stdin, without its newline. Interactive terminals get a
/// prompt on stderr (the password is echoed; prefer piping it).
fn read_password() -> anyhow::Result<String> {
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        eprint!("password (will be echoed; pipe it in to avoid that): ");
        std::io::stderr().flush()?;
    }
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .context("reading the password from stdin")?;
    let password = line.trim_end_matches(['\n', '\r']).to_owned();
    if password.is_empty() {
        bail!("no password on stdin; pipe it in, e.g. `rmf-pipes user add alice < password-file`");
    }
    Ok(password)
}
