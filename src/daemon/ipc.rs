//! Local IPC between the CLI and the daemon: newline-delimited JSON over a
//! loopback TCP socket. The daemon writes `daemon.json` (port + random token)
//! into the data dir; clients read it to connect and authenticate.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::config;
use crate::daemon::AppState;

#[derive(Serialize, Deserialize)]
struct DaemonInfo {
    pid: u32,
    port: u16,
    token: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    instance: String,
    #[serde(default)]
    build_id: String,
    #[serde(default)]
    ipc_version: u32,
    #[serde(default)]
    run_id: String,
}

#[derive(Serialize, Deserialize)]
struct Request {
    token: String,
    cmd: String,
    #[serde(default)]
    args: Value,
    channel: String,
    instance: String,
    ipc_version: u32,
}

#[derive(Serialize, Deserialize)]
struct Response {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn info_path() -> Result<PathBuf> {
    Ok(config::data_dir()?.join("daemon.json"))
}

pub struct IpcServer {
    run_id: String,
    port: u16,
    handle: JoinHandle<()>,
}

impl IpcServer {
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Stop accepting connections and remove the discovery file.
    pub async fn close(self) {
        self.handle.abort();
        if let Ok(path) = info_path()
            && crate::runtime::same_file_owner(&path, &self.run_id)
        {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Bind the IPC listener, publish `daemon.json`, and serve requests until
/// aborted via [`IpcServer::close`].
pub async fn serve(state: Arc<AppState>) -> Result<IpcServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let mut token_bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut token_bytes);
    let token: String = token_bytes.iter().map(|b| format!("{b:02x}")).collect();

    let info = DaemonInfo {
        channel: crate::runtime::CHANNEL.into(),
        instance: crate::runtime::context().instance.clone(),
        build_id: crate::runtime::BUILD_ID.into(),
        ipc_version: crate::runtime::IPC_VERSION,
        run_id: format!("{:016x}", rand::random::<u64>()),
        pid: std::process::id(),
        port,
        token: token.clone(),
    };
    crate::statefile::write_private(&info_path()?, &serde_json::to_vec(&info)?)?;

    let handle = tokio::spawn(async move {
        loop {
            let (socket, addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(error) => {
                    warn!(%error, "ipc accept failed");
                    continue;
                }
            };
            debug!(%addr, "ipc connection");
            let state = state.clone();
            let token = token.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_connection(socket, state, token).await {
                    debug!(%error, "ipc connection ended with error");
                }
            });
        }
    });

    Ok(IpcServer {
        port,
        handle,
        run_id: info.run_id,
    })
}

/// Longest request line accepted from a client; longer lines drop the
/// connection instead of buffering without bound (the peer may be
/// unauthenticated).
const MAX_LINE_BYTES: usize = 1024 * 1024;
/// How long a fresh connection has to deliver its first (authenticating) line.
const FIRST_LINE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Constant-time equality for the shared token.
fn token_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

/// Reads one `\n`-terminated line of at most `max` bytes. `Ok(None)` means
/// clean EOF; a line that exceeds `max` is an error.
async fn read_line_limited<R>(reader: &mut R, max: usize) -> Result<Option<String>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut buf = Vec::new();
    let read = (&mut *reader)
        .take(max as u64 + 1)
        .read_until(b'\n', &mut buf)
        .await?;
    if read == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
    } else if buf.len() > max {
        bail!("ipc request line exceeds {max} bytes");
    }
    Ok(Some(
        String::from_utf8(buf).context("ipc request is not UTF-8")?,
    ))
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    state: Arc<AppState>,
    token: String,
) -> Result<()> {
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = TokioBufReader::new(read_half);
    let mut first = true;

    loop {
        let line = if first {
            first = false;
            tokio::time::timeout(
                FIRST_LINE_TIMEOUT,
                read_line_limited(&mut reader, MAX_LINE_BYTES),
            )
            .await
            .context("ipc client sent no request in time")??
        } else {
            read_line_limited(&mut reader, MAX_LINE_BYTES).await?
        };
        let Some(line) = line else { break };
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request)
                if token_eq(&request.token, &token)
                    && request.channel == crate::runtime::CHANNEL
                    && request.instance == crate::runtime::context().instance
                    && request.ipc_version == crate::runtime::IPC_VERSION =>
            {
                match crate::daemon::dispatch(&request.cmd, request.args, &state).await {
                    Ok(data) => Response {
                        ok: true,
                        data: Some(data),
                        error: None,
                    },
                    Err(error) => Response {
                        ok: false,
                        data: None,
                        error: Some(format!("{error:#}")),
                    },
                }
            }
            Ok(_) => Response {
                ok: false,
                data: None,
                error: Some("invalid token".into()),
            },
            Err(error) => Response {
                ok: false,
                data: None,
                error: Some(format!("bad request: {error}")),
            },
        };
        let mut payload = serde_json::to_vec(&response)?;
        payload.push(b'\n');
        write_half.write_all(&payload).await?;
    }
    Ok(())
}

/// Send one request to the running daemon (synchronous; used from the CLI
/// process which has no tokio runtime).
pub fn client_request(cmd: &str, args: Value) -> Result<Value> {
    let path = info_path()?;
    let info: DaemonInfo = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("parsing daemon.json")?,
        Err(_) => bail!("daemon is not running (start it with `mistl daemon start`)"),
    };

    if info.channel != crate::runtime::CHANNEL
        || info.instance != crate::runtime::context().instance
        || info.ipc_version != crate::runtime::IPC_VERSION
    {
        bail!("daemon identity/protocol mismatch (legacy daemon must be stopped before migration)");
    }
    if crate::runtime::CHANNEL == "dev"
        && info.build_id != crate::runtime::BUILD_ID
        && !matches!(
            cmd,
            "daemon.status" | "daemon.stop" | "daemon.restart" | "network.status"
        )
    {
        bail!(
            "development daemon build differs from this CLI; stop that instance and start the rebuilt binary"
        );
    }

    let stream = match TcpStream::connect(("127.0.0.1", info.port)) {
        Ok(stream) => stream,
        Err(_) => {
            // Stale discovery file from a crashed daemon.
            if crate::runtime::same_file_owner(&path, &info.run_id) {
                let _ = std::fs::remove_file(&path);
            }
            bail!("daemon is not running (start it with `mistl daemon start`)");
        }
    };
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(30)))?;

    let request = Request {
        channel: crate::runtime::CHANNEL.into(),
        instance: crate::runtime::context().instance.clone(),
        ipc_version: crate::runtime::IPC_VERSION,
        token: info.token,
        cmd: cmd.to_string(),
        args,
    };
    let mut writer = stream.try_clone()?;
    let mut payload = serde_json::to_vec(&request)?;
    payload.push(b'\n');
    writer.write_all(&payload)?;
    writer.flush()?;

    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    let response: Response = serde_json::from_str(line.trim())
        .with_context(|| format!("bad response from daemon: {line}"))?;
    if response.ok {
        Ok(response.data.unwrap_or(Value::Null))
    } else {
        bail!(
            "{}",
            response
                .error
                .unwrap_or_else(|| "unknown daemon error".into())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_eq_matches_only_identical_tokens() {
        assert!(token_eq("abc", "abc"));
        assert!(!token_eq("abc", "abd"));
        assert!(!token_eq("abc", "abcd"));
        assert!(!token_eq("", "a"));
        assert!(token_eq("", ""));
    }

    #[tokio::test]
    async fn read_line_limited_splits_lines_and_caps_length() {
        let data = b"hello\nworld\n".to_vec();
        let mut reader = TokioBufReader::new(&data[..]);
        assert_eq!(
            read_line_limited(&mut reader, 16).await.unwrap().as_deref(),
            Some("hello")
        );
        assert_eq!(
            read_line_limited(&mut reader, 16).await.unwrap().as_deref(),
            Some("world")
        );
        assert!(read_line_limited(&mut reader, 16).await.unwrap().is_none());

        let long = [b'a'; 100];
        let mut reader = TokioBufReader::new(&long[..]);
        assert!(read_line_limited(&mut reader, 16).await.is_err());

        // Exactly `max` bytes plus the newline is still accepted.
        let exact = b"0123456789abcdef\n".to_vec();
        let mut reader = TokioBufReader::new(&exact[..]);
        assert_eq!(
            read_line_limited(&mut reader, 16)
                .await
                .unwrap()
                .unwrap()
                .len(),
            16
        );
    }
}
