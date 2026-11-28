//! Inspect only the routing field of a standard multipart upload. Keep byte
//! offsets into the original body so binary parts, ordering and headers are
//! never reconstructed. The legacy JSON adapter remains in api_server.

use std::ops::Range;

const MAX_CONTENT_TYPE: usize = 256; // v1 seq-zero metadata limit
const MAX_PART_HEADERS: usize = 16 * 1024;
const MAX_PARTS: usize = 1024;
const MAX_MODEL_BYTES: usize = 64 * 1024;
const INVALID: &str = "invalid multipart form";

pub(super) type Result<T> = std::result::Result<T, &'static str>;

fn params(mut rest: &str) -> Result<Vec<(&str, String)>> {
    let mut out = Vec::new();
    while !rest.trim().is_empty() {
        rest = rest
            .trim_start()
            .strip_prefix(';')
            .ok_or(INVALID)?
            .trim_start();
        if out.len() >= 64 {
            return Err(INVALID);
        }
        let (key, value) = rest.split_once('=').ok_or(INVALID)?;
        let key = key.trim();
        if key.is_empty()
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'*'))
        {
            return Err(INVALID);
        }
        let value = value.trim_start();
        if let Some(quoted) = value.strip_prefix('"') {
            let mut decoded = String::new();
            let mut escaped = false;
            let mut end = None;
            for (i, ch) in quoted.char_indices() {
                if escaped {
                    decoded.push(ch);
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    end = Some(i);
                    break;
                } else {
                    decoded.push(ch);
                }
            }
            rest = &quoted[end.ok_or(INVALID)? + 1..];
            out.push((key, decoded));
        } else {
            let end = value.find(';').unwrap_or(value.len());
            let token = value[..end].trim();
            if token.is_empty()
                || token
                    .chars()
                    .any(|c| c.is_control() || c.is_whitespace() || c == '"')
            {
                return Err(INVALID);
            }
            rest = &value[end..];
            out.push((key, token.to_string()));
        }
    }
    Ok(out)
}

fn unique_param<'a>(params: &'a [(&str, String)], key: &str) -> Result<Option<&'a str>> {
    let mut matches = params.iter().filter(|(k, _)| k.eq_ignore_ascii_case(key));
    let value = matches.next().map(|(_, v)| v.as_str());
    if matches.next().is_some() {
        return Err(INVALID);
    }
    Ok(value)
}

pub(super) fn is_multipart(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("multipart/form-data"))
}

fn boundary(content_type: &str) -> Result<String> {
    if content_type.len() > MAX_CONTENT_TYPE
        || reqwest::header::HeaderValue::from_str(content_type).is_err()
    {
        return Err(INVALID);
    }
    let end = content_type.find(';').ok_or(INVALID)?;
    if !content_type[..end]
        .trim()
        .eq_ignore_ascii_case("multipart/form-data")
    {
        return Err(INVALID);
    }
    let params = params(&content_type[end..])?;
    let boundary = unique_param(&params, "boundary")?.ok_or(INVALID)?;
    // RFC 2046: at most 70 bchars, with no trailing space.
    if !(1..=70).contains(&boundary.len())
        || boundary.ends_with(' ')
        || !boundary
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"'()+_,-./:=? ".contains(&b))
    {
        return Err(INVALID);
    }
    Ok(boundary.to_string())
}

fn find(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    bytes
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Return the next part's header start, or the closing delimiter.
fn delimiter_tail(body: &[u8], mut at: usize) -> Option<(usize, bool)> {
    let closing = body.get(at..)?.starts_with(b"--");
    if closing {
        at += 2;
    }
    while body.get(at).is_some_and(|b| matches!(b, b' ' | b'\t')) {
        at += 1;
    }
    if body.get(at..)?.starts_with(b"\r\n") {
        Some((at + 2, closing))
    } else if closing && at == body.len() {
        Some((at, true))
    } else {
        None
    }
}

/// A boundary must occupy a delimiter line, not just appear in binary data.
fn next_delimiter(body: &[u8], marker: &[u8], mut at: usize) -> Option<(usize, usize, bool)> {
    while let Some(offset) = find(body.get(at..)?, marker) {
        let start = at + offset;
        if let Some((next, closing)) = delimiter_tail(body, start + marker.len()) {
            return Some((start, next, closing));
        }
        at = start + 1;
    }
    None
}

fn is_model(headers: &[u8]) -> Result<bool> {
    let headers = std::str::from_utf8(headers).map_err(|_| INVALID)?;
    let mut disposition = None;
    let mut content_type = None;
    let mut transfer_encoding = false;
    for line in headers.split("\r\n") {
        let (key, value) = line.split_once(':').ok_or(INVALID)?;
        if key.eq_ignore_ascii_case("content-disposition") {
            if disposition.replace(value.trim()).is_some() {
                return Err(INVALID);
            }
        } else if key.eq_ignore_ascii_case("content-type") {
            if content_type.replace(value.trim()).is_some() {
                return Err(INVALID);
            }
        } else if key.eq_ignore_ascii_case("content-transfer-encoding") {
            transfer_encoding = true;
        } else if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
            return Err(INVALID);
        }
    }
    let disposition = disposition.ok_or(INVALID)?;
    let end = disposition.find(';').ok_or(INVALID)?;
    if !disposition[..end].trim().eq_ignore_ascii_case("form-data") {
        return Err(INVALID);
    }
    let params = params(&disposition[end..])?;
    let name = unique_param(&params, "name")?.ok_or(INVALID)?;
    if unique_param(&params, "name*")?.is_some() {
        return Err(INVALID);
    }
    if name != "model" {
        return Ok(false);
    }
    if unique_param(&params, "filename")?.is_some()
        || unique_param(&params, "filename*")?.is_some()
        || transfer_encoding
        || content_type.is_some_and(|ct| {
            !ct.split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .eq_ignore_ascii_case("text/plain")
        })
    {
        return Err("model must be a text part");
    }
    Ok(true)
}

pub(super) struct Form {
    pub boundary: String,
    model: Option<Range<usize>>,
}

impl Form {
    pub fn parse(body: &[u8], content_type: &str) -> Result<Self> {
        let boundary = boundary(content_type)?;
        let marker = format!("\r\n--{boundary}");
        let marker = marker.as_bytes();
        let mut model = None;
        let (mut at, mut closing) = if body.starts_with(&marker[2..]) {
            delimiter_tail(body, marker.len() - 2).ok_or(INVALID)?
        } else {
            let (_, next, closing) = next_delimiter(body, marker, 0).ok_or(INVALID)?;
            (next, closing)
        };
        for _ in 0..MAX_PARTS {
            if closing {
                return Ok(Self { boundary, model });
            }
            let header_limit = body.len().min(at.saturating_add(MAX_PART_HEADERS + 4));
            let head_end = at + find(&body[at..header_limit], b"\r\n\r\n").ok_or(INVALID)?;
            let payload_start = head_end + 4;
            let (payload_end, next, last) =
                next_delimiter(body, marker, payload_start).ok_or(INVALID)?;
            if is_model(&body[at..head_end])? {
                if model.is_some() {
                    return Err("duplicate multipart model");
                }
                let text = &body[payload_start..payload_end];
                if text.len() > MAX_MODEL_BYTES
                    || std::str::from_utf8(text)
                        .map_err(|_| INVALID)?
                        .chars()
                        .any(char::is_control)
                {
                    return Err("invalid multipart model");
                }
                model = Some(payload_start..payload_end);
            }
            at = next;
            closing = last;
        }
        if closing {
            Ok(Self { boundary, model })
        } else {
            Err("too many multipart parts")
        }
    }

    pub fn model<'a>(&self, body: &'a [u8]) -> Option<&'a str> {
        let range = self.model.clone()?;
        std::str::from_utf8(body.get(range)?)
            .ok()
            .filter(|m| !m.trim().is_empty())
    }

    pub fn rewrite_model(&self, mut body: Vec<u8>, model: &str, cap: usize) -> Result<Vec<u8>> {
        let range = self.model.clone().ok_or("model is required")?;
        if model.trim().is_empty()
            || model.len() > MAX_MODEL_BYTES
            || model.chars().any(char::is_control)
        {
            return Err("invalid resolved model");
        }
        if body.len() - range.len() + model.len() > cap {
            return Err("request_too_large");
        }
        if body[range.clone()] != *model.as_bytes() {
            body.splice(range, model.bytes());
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_binary_and_quoted_headers_and_rewrites_only_model() {
        let body = b"--B\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"one;two.png\"\r\n\r\n\x00--B\xff\r\n--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nalias\r\n--B--\r\n";
        let form = Form::parse(body, "multipart/form-data; boundary=\"B\"").unwrap();
        assert_eq!(form.model(body), Some("alias"));
        let rewritten = form
            .rewrite_model(body.to_vec(), "upstream-model", 1024)
            .unwrap();
        let expected = [
            &body[..body.len() - 14],
            b"upstream-model\r\n--B--\r\n".as_slice(),
        ]
        .concat();
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn validates_model_and_form_limits() {
        let content_type = "multipart/form-data; boundary=B";
        let part = b"--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nalias\r\n";
        let duplicate = [part.as_slice(), part, b"--B--\r\n"].concat();
        assert!(Form::parse(&duplicate, content_type).is_err());
        assert!(Form::parse(part, content_type).is_err());
        let body = [part.as_slice(), b"--B--\r\n"].concat();
        let form = Form::parse(&body, content_type).unwrap();
        assert_eq!(
            form.rewrite_model(body.clone(), "longer-model", body.len()),
            Err("request_too_large")
        );
        assert!(Form::parse(&body, "multipart/form-data; boundary=B; boundary=C").is_err());
    }
}
