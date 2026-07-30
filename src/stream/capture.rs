//! Screen capture via `ffmpeg` (Windows `gdigrab`), encoding H264 and
//! muxing it into MPEG-TS over a local UDP socket, mirroring mistlink's
//! capture -> local transport pipeline (there implemented with
//! `pion/mediadevices`; here with a plain `ffmpeg` child process since
//! there's no equivalent screen-capture crate declared for this binary).

use anyhow::{Context, Result, bail};
use tokio::process::{Child, Command};

/// A running `ffmpeg` screen-capture process.
pub struct Capture {
    child: Child,
}

impl Capture {
    /// Spawn `ffmpeg` capturing the desktop at `frame_rate` fps and sending
    /// H264/MPEG-TS to `127.0.0.1:{ingest_port}`. Returns a clear error if
    /// `ffmpeg` isn't on `PATH`.
    ///
    /// Audio capture is not implemented in this build; if `audio_capture` is
    /// set, video-only capture proceeds and a warning is logged.
    pub async fn spawn(frame_rate: u32, ingest_port: u16, audio_capture: bool) -> Result<Self> {
        check_ffmpeg_available().await?;

        if audio_capture {
            tracing::warn!(
                "stream.audio_capture is enabled, but audio capture is not implemented in \
                 this build yet; continuing with video only"
            );
        }

        // Closed GOP twice the frame rate, matching mistlink's keyframe
        // cadence so a freshly-connected client isn't stuck waiting long
        // for an IDR frame.
        let gop = frame_rate.saturating_mul(2).max(1);

        let mut command = Command::new("ffmpeg");
        command
            .args([
                "-f",
                "gdigrab",
                "-framerate",
                &frame_rate.to_string(),
                "-i",
                "desktop",
            ])
            .args([
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-tune",
                "zerolatency",
            ])
            .args(["-pix_fmt", "yuv420p", "-bf", "0", "-g", &gop.to_string()])
            .args(["-f", "mpegts", &format!("udp://127.0.0.1:{ingest_port}")])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        #[cfg(windows)]
        {
            // `tokio::process::Command` exposes `creation_flags` as an
            // inherent method on Windows (unlike `std::process::Command`,
            // which needs `std::os::windows::process::CommandExt`).
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }

        let mut child = command.spawn().context("spawning ffmpeg")?;

        // Drain stderr in the background (ffmpeg logs everything there) so
        // the pipe never fills up and blocks the encoder; surface lines at
        // debug level for troubleshooting capture issues.
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut lines = BufReader::new(stderr).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => {
                            tracing::debug!(target: "mistl::stream::ffmpeg", "{line}")
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            });
        }

        Ok(Self { child })
    }

    /// Kill the `ffmpeg` child and wait for it to exit.
    pub async fn stop(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        // Best-effort; `kill_on_drop(true)` above also covers the case
        // where the tokio runtime tears the child down for us.
        let _ = self.child.start_kill();
    }
}

async fn check_ffmpeg_available() -> Result<()> {
    match Command::new("ffmpeg")
        .arg("-version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
    {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            bail!("`ffmpeg -version` exited with {status}; is ffmpeg installed correctly?")
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!("ffmpeg was not found on PATH; install ffmpeg to use `mistl stream start`")
        }
        Err(error) => Err(error).context("checking for ffmpeg on PATH"),
    }
}
