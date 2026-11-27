//! Scoped bearer-authenticated control plane on the dashboard listener.

use std::io;
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use super::server::{RequestHead, percent_decode, read_body};
use crate::authz::{self, Denied, Scope};
use crate::daemon::AppState;

const MAX_BODY_BYTES: u64 = 64 * 1024;
const GRANT_KEYS: &[&str] = &["expires_at", "ttl_secs", "note", "limits"];
const TOKEN_KEYS: &[&str] = &["name", "models", "limits", "expires_at", "ttl_secs"];
const CAP_KEYS: &[&str] = &[
    "aud",
    "service",
    "actions",
    "resource",
    "ttl_secs",
    "expires_at",
    "limits",
    "to",
    "deliver",
];

struct ApiError {
    status: u16,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(400, "invalid_request", message)
    }

    fn not_found() -> Self {
        Self::new(404, "not_found", "Not found")
    }

    fn method() -> Self {
        Self::new(405, "method_not_allowed", "Method not allowed")
    }
}

impl From<Denied> for ApiError {
    fn from(denied: Denied) -> Self {
        Self::new(denied.http_status(), denied.code, denied.message)
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        // Expose validation messages, not filesystem paths or error chains
        // (e.g. persistence and identity-loading failures).
        let message = error.to_string();
        if message == "unknown API token id" {
            Self::not_found()
        } else if error.chain().any(|cause| cause.is::<io::Error>())
            || message.contains(['/', '\\'])
        {
            Self::invalid("Authorization command failed")
        } else {
            Self::invalid(message)
        }
    }
}

enum Route<'a> {
    Info,
    Usage,
    Events,
    Grants,
    Grant(&'a str, &'a str),
    Tokens,
    Token(&'a str),
    Caps,
    Cap(&'a str),
    Unknown,
}

impl<'a> Route<'a> {
    fn parse(path: &'a str) -> Self {
        let Some(suffix) = path.strip_prefix("/service/v1/") else {
            return Self::Unknown;
        };
        match suffix.split('/').collect::<Vec<_>>().as_slice() {
            ["info"] => Self::Info,
            ["usage"] => Self::Usage,
            ["usage", "events"] => Self::Events,
            ["grants"] => Self::Grants,
            ["grants", service, subject] if !service.is_empty() && !subject.is_empty() => {
                Self::Grant(service, subject)
            }
            ["tokens"] => Self::Tokens,
            ["tokens", id] if !id.is_empty() => Self::Token(id),
            ["caps"] => Self::Caps,
            ["caps", id] if !id.is_empty() => Self::Cap(id),
            _ => Self::Unknown,
        }
    }

    fn scope(&self) -> Scope {
        match self {
            Self::Info | Self::Usage | Self::Events | Self::Unknown => Scope::Usage,
            _ => Scope::Manage,
        }
    }
}

fn service(value: &str) -> Result<&str, ApiError> {
    match value {
        "ai" | "escrow" => Ok(value),
        _ => Err(ApiError::invalid("service must be ai or escrow")),
    }
}

// Unlike the dashboard query helper, report malformed values rather than
// treating them as absent and silently using the defaults.
fn query_param(query: &str, key: &str) -> Result<Option<String>, ApiError> {
    let mut result = None;
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name == key {
            if result.is_some() {
                return Err(ApiError::invalid(format!(
                    "duplicate `{key}` query parameter"
                )));
            }
            result =
                Some(percent_decode(value).ok_or_else(|| {
                    ApiError::invalid(format!("invalid `{key}` query parameter"))
                })?);
        }
    }
    Ok(result)
}

fn service_filter(query: &str) -> Result<Value, ApiError> {
    match query_param(query, "service")? {
        Some(value) => Ok(json!({"service": service(&value)?})),
        None => Ok(json!({})),
    }
}

fn event_query(query: &str) -> Result<Value, ApiError> {
    let after = query_param(query, "after")?
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ApiError::invalid("after must be a u64"))
        })
        .transpose()?
        .unwrap_or(0);
    let limit = query_param(query, "limit")?
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ApiError::invalid("limit must be between 1 and 1000"))
        })
        .transpose()?
        .unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(ApiError::invalid("limit must be between 1 and 1000"));
    }
    Ok(json!({"after": after, "limit": limit}))
}

async fn body_args(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    allowed: &[&str],
) -> Result<Value, ApiError> {
    if head.header("transfer-encoding").is_some() {
        return Err(ApiError::invalid("Transfer-Encoding is not supported"));
    }
    let length = head
        .header("content-length")
        .ok_or_else(|| ApiError::new(411, "length_required", "Content-Length is required"))?
        .parse::<u64>()
        .map_err(|_| ApiError::invalid("invalid Content-Length"))?;
    if length > MAX_BODY_BYTES {
        return Err(ApiError::new(
            413,
            "payload_too_large",
            "JSON body exceeds 64 KiB",
        ));
    }
    if !head.header("content-type").is_some_and(|value| {
        value
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("application/json")
    }) {
        return Err(ApiError::new(
            415,
            "unsupported_media_type",
            "Content-Type must be application/json",
        ));
    }
    let bytes = read_body(stream, leftover, length as usize)
        .await
        .map_err(|_| ApiError::invalid("incomplete request body"))?;
    let args: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::invalid("body must be a valid JSON object"))?;
    let object = args
        .as_object()
        .ok_or_else(|| ApiError::invalid("body must be a JSON object"))?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(ApiError::invalid("unknown body key"));
    }
    Ok(args)
}

async fn command(cmd: &str, args: Value, state: &Arc<AppState>) -> Result<Value, ApiError> {
    Ok(authz::handle(cmd, args, state).await?)
}

fn infer_only(info: &Value) -> bool {
    info["scopes"] == json!(["infer"])
}

async fn infer_tokens(state: &Arc<AppState>) -> Result<Vec<Value>, ApiError> {
    let mut tokens = command("authz.token.list", json!({}), state).await?;
    let tokens = tokens
        .as_array_mut()
        .ok_or_else(|| ApiError::new(500, "internal_error", "Invalid token list"))?;
    tokens.retain(infer_only);
    Ok(std::mem::take(tokens))
}

async fn require_infer_token(id: &str, state: &Arc<AppState>) -> Result<(), ApiError> {
    if infer_tokens(state)
        .await?
        .iter()
        .any(|info| info["id"].as_str() == Some(id))
    {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

async fn dispatch(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: &Arc<AppState>,
) -> Result<(u16, Value), ApiError> {
    let (path, query) = head.path.split_once('?').unwrap_or((&head.path, ""));
    let route = Route::parse(path);
    authz::authorize_service(head.header("authorization"), route.scope())?;

    let method = head.method.to_ascii_uppercase();
    if method == "OPTIONS" {
        return Err(ApiError::method());
    }
    let result = match (route, method.as_str()) {
        (Route::Info, "GET") => {
            let show = command("authz.show", json!({}), state).await?;
            json!({"api": 1, "did": show["local"]["did"], "node_id": show["local"]["node"],
                "services": {"ai": {"mode": show["policy"]["ai"]["mode"]},
                             "escrow": {"mode": show["policy"]["escrow"]["mode"]}},
                "require_token": show["policy"]["api"]["require_token"]})
        }
        (Route::Usage, "GET") => command("authz.usage", service_filter(query)?, state).await?,
        (Route::Events, "GET") => command("authz.events", event_query(query)?, state).await?,
        (Route::Grants, "GET") => {
            command("authz.grant.list", service_filter(query)?, state).await?
        }
        (Route::Grant(svc, subject), "PUT" | "DELETE") => {
            let svc = service(svc)?;
            let subject = percent_decode(subject)
                .ok_or_else(|| ApiError::invalid("invalid subject encoding"))?;
            let mut args = if method == "PUT" {
                body_args(stream, head, leftover, GRANT_KEYS).await?
            } else {
                json!({})
            };
            args["service"] = json!(svc);
            args["subject"] = json!(subject);
            let cmd = if method == "PUT" {
                "authz.grant.put"
            } else {
                "authz.grant.remove"
            };
            command(cmd, args, state).await?
        }
        (Route::Tokens, "GET") => json!(infer_tokens(state).await?),
        (Route::Tokens, "POST") => {
            let mut args = body_args(stream, head, leftover, TOKEN_KEYS).await?;
            args["scopes"] = json!(["infer"]);
            return Ok((201, command("authz.token.create", args, state).await?));
        }
        (Route::Token(id), "PATCH" | "DELETE") => {
            require_infer_token(id, state).await?;
            let mut args = if method == "PATCH" {
                body_args(stream, head, leftover, TOKEN_KEYS).await?
            } else {
                json!({})
            };
            args["id"] = json!(id);
            let cmd = if method == "PATCH" {
                "authz.token.update"
            } else {
                "authz.token.revoke"
            };
            command(cmd, args, state).await?
        }
        (Route::Caps, "GET") => command("authz.cap.list", json!({}), state).await?,
        (Route::Caps, "POST") => {
            let args = body_args(stream, head, leftover, CAP_KEYS).await?;
            if args.get("deliver").is_some_and(|value| !value.is_boolean()) {
                return Err(ApiError::invalid("deliver must be a boolean"));
            }
            return Ok((201, command("authz.cap.issue", args, state).await?));
        }
        (Route::Cap(id), "DELETE") => command("authz.cap.revoke", json!({"id": id}), state).await?,
        (Route::Unknown, _) => return Err(ApiError::not_found()),
        _ => return Err(ApiError::method()),
    };
    Ok((200, result))
}

pub(super) async fn handle(
    stream: &mut TcpStream,
    head: &RequestHead,
    leftover: Vec<u8>,
    state: Arc<AppState>,
) -> io::Result<()> {
    let (status, body) = match dispatch(stream, head, leftover, &state).await {
        Ok(result) => result,
        Err(error) => (
            error.status,
            json!({"error": {"code": error.code, "message": error.message}}),
        ),
    };
    let reason = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        411 => "Length Required",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        429 => "Too Many Requests",
        _ => "Internal Server Error",
    };
    let body = serde_json::to_vec(&body).map_err(io::Error::other)?;
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    // Reset even when an assertion fails, while TEST_LOCK is held.
    struct ResetStore;

    impl ResetStore {
        fn new() -> Self {
            authz::test_reset();
            Self
        }
    }

    impl Drop for ResetStore {
        fn drop(&mut self) {
            authz::test_reset();
        }
    }

    fn token(scopes: &[&str]) -> (String, String) {
        let created = authz::test_command(
            "authz.token.create",
            json!({"name": "test", "scopes": scopes}),
        )
        .unwrap();
        (
            format!("Bearer {}", created["token"].as_str().unwrap()),
            created["info"]["id"].as_str().unwrap().to_owned(),
        )
    }

    async fn request(
        method: &str,
        path: &str,
        auth: Option<&str>,
        headers: &str,
        body: &str,
    ) -> (u16, Value) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            super::super::server::handle_connection_inner(
                &mut socket,
                AppState::for_test(),
                "test-instance",
                "dashboard-token",
            )
            .await
            .unwrap();
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let authorization = auth
            .map(|value| format!("Authorization: {value}\r\n"))
            .unwrap_or_default();
        // No cookie, dashboard header or loopback Host: the Service API is
        // independently authenticated and may sit behind a reverse proxy.
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: service.example\r\n{authorization}{headers}\r\n{body}"
        );
        client.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        server.await.unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(headers.contains("Cache-Control: no-store"));
        assert!(!headers.to_ascii_lowercase().contains("access-control-"));
        let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
        (status, serde_json::from_str(body).unwrap())
    }

    async fn json_request(method: &str, path: &str, auth: &str, body: Value) -> (u16, Value) {
        let body = body.to_string();
        let headers = format!(
            "Content-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n",
            body.len()
        );
        request(method, path, Some(auth), &headers, &body).await
    }

    fn assert_error(response: (u16, Value), status: u16, code: &str) {
        assert_eq!(response.0, status);
        assert_eq!(response.1["error"]["code"], code);
        assert!(response.1["error"]["message"].is_string());
    }

    #[tokio::test]
    async fn missing_or_invalid_token_requires_auth_even_on_unknown_paths() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        for path in ["/service/v1", "/service/v1/info", "/service/v1/unknown"] {
            assert_error(
                request("GET", path, None, "", "").await,
                401,
                "unauthorized",
            );
            assert_error(
                request("GET", path, Some("Bearer invalid"), "", "").await,
                401,
                "unauthorized",
            );
        }
    }

    #[tokio::test]
    async fn infer_token_cannot_use_service_api() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        let (infer, _) = token(&["infer"]);
        for path in [
            "/service/v1/info",
            "/service/v1/usage",
            "/service/v1/grants",
        ] {
            assert_error(
                request("GET", path, Some(&infer), "", "").await,
                403,
                "scope_required",
            );
        }
    }

    #[tokio::test]
    async fn usage_scope_can_read_usage_and_events_but_not_grants() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        let (usage, _) = token(&["usage"]);
        let (infer, id) = token(&["infer"]);
        authz::authorize_api(Some(&infer), Some("example-model"))
            .unwrap()
            .with_op("chat")
            .record(1, 12);
        let (status, body) =
            request("GET", "/service/v1/usage?service=ai", Some(&usage), "", "").await;
        assert_eq!(status, 200);
        assert_eq!(body["ai"][0]["token"], id);
        let (status, body) = request(
            "GET",
            "/service/v1/usage/events?after=0&limit=1",
            Some(&usage),
            "",
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["events"][0]["token"], id);
        assert_eq!(body["next"], 1);
        assert_eq!(body["gap"], false);
        assert_error(
            request("GET", "/service/v1/grants", Some(&usage), "", "").await,
            403,
            "scope_required",
        );
        assert_error(
            json_request(
                "PUT",
                "/service/v1/grants/ai/0123456789abcdef",
                &usage,
                json!({}),
            )
            .await,
            403,
            "scope_required",
        );
    }

    #[tokio::test]
    async fn manage_scope_grants_round_trip_with_percent_encoded_did() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        let (manage, _) = token(&["manage"]);
        let identity = crate::identity::for_test();
        let did = identity.did();
        let path = format!("/service/v1/grants/ai/{}", did.replace(':', "%3A"));
        let (status, grant) = json_request(
            "PUT",
            &path,
            &manage,
            json!({"note":"member", "ttl_secs":600, "limits":{"max_requests":10}}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(grant["subject"], did);
        assert_eq!(grant["limits"]["max_requests"], 10);
        let (status, grants) = request(
            "GET",
            "/service/v1/grants?service=ai",
            Some(&manage),
            "",
            "",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(grants, json!([grant]));
        let (status, replacement) = json_request("PUT", &path, &manage, json!({})).await;
        assert_eq!(status, 200);
        assert!(replacement["limits"].is_null());
        assert!(replacement["expires_at"].is_null());
        let (status, result) = request("DELETE", &path, Some(&manage), "", "").await;
        assert_eq!((status, result), (200, json!({"removed":true})));
        let (status, grants) = request("GET", "/service/v1/grants", Some(&manage), "", "").await;
        assert_eq!((status, grants), (200, json!([])));
    }

    #[tokio::test]
    async fn manage_creates_infer_token_and_can_patch_and_revoke_it() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        let (manage, _) = token(&["manage"]);
        let (status, result) = json_request(
            "POST",
            "/service/v1/tokens",
            &manage,
            json!({"name":"customer", "models":["example-model"]}),
        )
        .await;
        assert_eq!(status, 201);
        assert_eq!(result["info"]["scopes"], json!(["infer"]));
        assert!(result["info"].get("hash").is_none());
        let credential = format!("Bearer {}", result["token"].as_str().unwrap());
        let id = result["info"]["id"].as_str().unwrap();
        assert!(authz::authorize_api(Some(&credential), Some("example-model")).is_ok());
        let path = format!("/service/v1/tokens/{id}");
        let (status, patched) = json_request(
            "PATCH",
            &path,
            &manage,
            json!({"name":"renewed", "ttl_secs":600}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(patched["name"], "renewed");
        assert!(patched["expires_at"].is_string());
        let (status, revoked) = request("DELETE", &path, Some(&manage), "", "").await;
        assert_eq!(status, 200);
        assert!(revoked["revoked_at"].is_string());
        assert!(authz::authorize_api(Some(&credential), Some("example-model")).is_err());
    }

    #[tokio::test]
    async fn management_and_mixed_scope_tokens_are_hidden_and_untouchable() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        let (manage, manage_id) = token(&["manage"]);
        let (usage, usage_id) = token(&["usage"]);
        let (mixed, mixed_id) = token(&["infer", "usage"]);
        let (_, infer_id) = token(&["infer"]);
        let (status, list) = request("GET", "/service/v1/tokens", Some(&manage), "", "").await;
        assert_eq!(status, 200);
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["id"], infer_id);
        for id in [manage_id, usage_id, mixed_id, "nonexistent".into()] {
            let path = format!("/service/v1/tokens/{id}");
            assert_error(
                request("DELETE", &path, Some(&manage), "", "").await,
                404,
                "not_found",
            );
            assert_error(
                json_request("PATCH", &path, &manage, json!({"name":"changed"})).await,
                404,
                "not_found",
            );
        }
        for credential in [&manage, &usage, &mixed] {
            assert!(authz::authorize_service(Some(credential), Scope::Usage).is_ok());
        }
        assert_error(
            json_request(
                "POST",
                "/service/v1/tokens",
                &manage,
                json!({"name":"customer", "scopes":["manage"]}),
            )
            .await,
            400,
            "invalid_request",
        );
    }

    #[tokio::test]
    async fn validates_body_headers_json_and_allowed_keys() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        let (manage, _) = token(&["manage"]);
        let path = "/service/v1/grants/ai/0123456789abcdef";
        for (headers, body, status, code) in [
            (
                "Content-Type: application/json\r\n",
                "",
                411,
                "length_required",
            ),
            (
                "Content-Length: 65537\r\nContent-Type: application/json\r\n",
                "",
                413,
                "payload_too_large",
            ),
            (
                "Content-Length: 2\r\nContent-Type: text/plain\r\n",
                "{}",
                415,
                "unsupported_media_type",
            ),
            (
                "Content-Length: 2\r\nContent-Type: application/json\r\n",
                "[]",
                400,
                "invalid_request",
            ),
            (
                "Content-Length: 1\r\nContent-Type: application/json\r\n",
                "{",
                400,
                "invalid_request",
            ),
        ] {
            assert_error(
                request("PUT", path, Some(&manage), headers, body).await,
                status,
                code,
            );
        }
        assert_error(
            json_request("PUT", path, &manage, json!({"subject":"other"})).await,
            400,
            "invalid_request",
        );
        assert_error(
            json_request("POST", "/service/v1/caps", &manage, json!({"local":true})).await,
            400,
            "invalid_request",
        );
    }

    #[tokio::test]
    async fn rejects_unknown_routes_wrong_methods_and_invalid_query_parameters() {
        let _lock = authz::TEST_LOCK.lock().await;
        let _reset = ResetStore::new();
        let (manage, _) = token(&["manage"]);
        for (method, path, status, code) in [
            ("GET", "/service/v1", 404, "not_found"),
            ("GET", "/service/v1/unknown", 404, "not_found"),
            ("POST", "/service/v1/usage", 405, "method_not_allowed"),
            ("OPTIONS", "/service/v1/tokens", 405, "method_not_allowed"),
            (
                "GET",
                "/service/v1/usage?service=other",
                400,
                "invalid_request",
            ),
            (
                "GET",
                "/service/v1/usage/events?after=-1",
                400,
                "invalid_request",
            ),
            (
                "GET",
                "/service/v1/usage/events?limit=0",
                400,
                "invalid_request",
            ),
            (
                "GET",
                "/service/v1/usage/events?limit=1001",
                400,
                "invalid_request",
            ),
            (
                "GET",
                "/service/v1/usage/events?after=%zz",
                400,
                "invalid_request",
            ),
            (
                "DELETE",
                "/service/v1/grants/other/0123456789abcdef",
                400,
                "invalid_request",
            ),
            (
                "DELETE",
                "/service/v1/grants/ai/did%zz",
                400,
                "invalid_request",
            ),
        ] {
            assert_error(
                request(method, path, Some(&manage), "", "").await,
                status,
                code,
            );
        }
    }
}
