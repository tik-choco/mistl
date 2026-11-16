//! Clipboard helpers. Terminal writes belong to the TUI event loop.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{Result, anyhow};
use base64::Engine;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const MAX_OSC52_BYTES: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Native,
    Terminal,
}

/// Prefer the OS clipboard, with a single deadline for all native attempts.
/// A terminal fallback requires the caller to write `osc52(text)` separately.
pub async fn copy(text: &str) -> Result<Method> {
    let native = tokio::time::timeout(Duration::from_secs(2), copy_native(text)).await;
    match native {
        Ok(Ok(())) => Ok(Method::Native),
        _ if text.len() <= MAX_OSC52_BYTES => Ok(Method::Terminal),
        Ok(Err(e)) => Err(anyhow!(
            "clipboard unavailable: {e}; text exceeds terminal copy limit"
        )),
        Err(_) => Err(anyhow!(
            "clipboard timed out; text exceeds terminal copy limit"
        )),
    }
}

async fn copy_native(text: &str) -> Result<()> {
    #[cfg(windows)]
    {
        run("clip.exe", &[], &utf16le(text)).await
    }
    #[cfg(target_os = "macos")]
    {
        run("pbcopy", &[], text.as_bytes()).await
    }
    #[cfg(not(any(windows, target_os = "macos")))]
    {
        for (program, args) in [
            ("wl-copy", &[][..]),
            ("xclip", &["-selection", "clipboard"][..]),
            ("xsel", &["--clipboard", "--input"][..]),
        ] {
            if run(program, args, text.as_bytes()).await.is_ok() {
                return Ok(());
            }
        }
        Err(anyhow!("no native clipboard helper succeeded"))
    }
}

async fn run(program: &str, args: &[&str], bytes: &[u8]) -> Result<()> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    let mut child = command.spawn()?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("no clipboard stdin"))?;
    stdin.write_all(bytes).await?;
    drop(stdin);
    if !child.wait().await?.success() {
        return Err(anyhow!("clipboard helper failed"));
    }
    Ok(())
}

/// clip.exe detects UTF-16LE without a BOM and would paste a BOM verbatim
/// as U+FEFF; it also turns LF into CRLF itself.
#[cfg(any(windows, test))]
fn utf16le(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

pub fn osc52(text: &str) -> Option<String> {
    (text.len() <= MAX_OSC52_BYTES).then(|| {
        format!(
            "\x1b]52;c;{}\x07",
            base64::engine::general_purpose::STANDARD.encode(text)
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_encoding_round_trips_japanese_and_surrogates() {
        let text = "日本語のコピー\nEnglish 🦀";
        let encoded = utf16le(text);
        assert_ne!(&encoded[..2], &[0xff, 0xfe], "no BOM");
        let words: Vec<u16> = encoded
            .chunks_exact(2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
            .collect();
        assert_eq!(String::from_utf16(&words).unwrap(), text);
        assert!(utf16le("").is_empty());
    }

    #[test]
    fn osc52_frames_base64_and_bounds_utf8_bytes() {
        assert_eq!(osc52("hello").unwrap(), "\x1b]52;c;aGVsbG8=\x07");
        assert_eq!(osc52("").unwrap(), "\x1b]52;c;\x07");
        let text = "日本語\n🦀";
        let frame = osc52(text).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(
                frame
                    .strip_prefix("\x1b]52;c;")
                    .unwrap()
                    .strip_suffix('\x07')
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(decoded, text.as_bytes());
        assert!(osc52(&"x".repeat(MAX_OSC52_BYTES)).is_some());
        assert!(osc52(&"界".repeat(MAX_OSC52_BYTES / 3 + 1)).is_none());
    }
}
