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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
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
}

#[derive(Serialize, Deserialize)]
struct Request {
    token: String,
    cmd: String,
    #[serde(default)]
    args: Value,
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
        if let Ok(path) = info_path() {
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
        pid: std::process::id(),
        port,
        token: token.clone(),
    };
    std::fs::write(info_path()?, serde_json::to_vec(&info)?)?;

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

    Ok(IpcServer { port, handle })
}

async fn handle_connection(
    socket: tokio::net::TcpStream,
    state: Arc<AppState>,
    token: String,
) -> Result<()> {
    let (read_half, mut write_half) = socket.into_split();
    let mut lines = TokioBufReader::new(read_half).lines();

    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) if request.token == token => {
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

    let stream = match TcpStream::connect(("127.0.0.1", info.port)) {
        Ok(stream) => stream,
        Err(_) => {
            // Stale discovery file from a crashed daemon.
            let _ = std::fs::remove_file(&path);
            bail!("daemon is not running (start it with `mistl daemon start`)");
        }
    };

    let request = Request {
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
            response.error.unwrap_or_else(|| "unknown daemon error".into())
        )
    }
}
